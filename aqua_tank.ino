/*
 * MẠCH TỰ ĐỘNG GIÁM SÁT MÔI TRƯỜNG BỂ NUÔI THỦY, HẢI SẢN
 * ESP32 + HC-SR04 (mực nước) + DS18B20 (nhiệt độ bể) + máy bơm vào (PWM) + van thoát ra (relay)
 * FreeRTOS 3 task, MQTT (broker EMQX)
 *
 * Kiến trúc:
 *   Task đọc data    (Core 1, ưu tiên 2): mỗi 1 s đo khoảng cách, tính mực nước, đọc nhiệt độ,
 *                                         in Serial, gửi qAlert (mực nước) và qData (đầy đủ)
 *   Task cảnh báo    (Core 1, ưu tiên 3): nhận qAlert + qLenh, là task DUY NHẤT điều khiển bơm và van
 *   Task truyền data (Core 0, ưu tiên 1): kết nối WiFi, chờ cờ semWiFi, kết nối MQTT, gửi dữ liệu, nhận lệnh từ web
 *
 * Đối tượng giao tiếp:
 *   qData      (Queue 5 ô)  : Task đọc     -> Task truyền   ("data Queue": khoảng cách, mực nước, nhiệt độ)
 *   qAlert     (Queue 5 ô)  : Task đọc     -> Task cảnh báo ("data Alert": mực nước)
 *   qLenh      (Queue 3 ô)  : Task truyền  -> Task cảnh báo (lệnh nút bấm web)
 *   qTrangThai (Queue 1 ô)  : Task cảnh báo-> Task truyền   (trạng thái bơm/van, hộp thư ghi đè)
 *   semWiFi    (Binary sem) : sự kiện WiFi -> Task truyền   ("kết nối WiFi thành công")
 *   mSerial    (Mutex)      : khóa Serial dùng chung giữa Task đọc (Core 1) và Task truyền (Core 0)
 *
 * Mực nước: cảm biến HC-SR04 gắn ở nắp bể, cách đáy TANK_HEIGHT_CM, đo xuống mặt nước:
 *   mực nước (m) = (TANK_HEIGHT_CM - khoảng cách) / 100
 *
 * Thuật toán điều khiển (chạy khi mực nước VỪA ĐI VÀO một vùng):
 *   1,0 m  <= mực nước < 1,5 m  -> bơm 50% công suất trong 10 s
 *   1,5 m  <= mực nước <= 2,0 m -> bơm 100% công suất trong 20 s
 *   mực nước < 0,5 m            -> mở van thoát trong 10 s
 *   các vùng còn lại             -> không làm gì
 *   Bơm và van không bao giờ chạy cùng lúc. Mỗi lần vào vùng chỉ chạy một lượt,
 *   phải rời vùng rồi vào lại mới chạy lượt mới (tránh bơm/xả liên tục).
 * Chế độ thủ công (nút web): bật bơm thì đóng van và ngược lại; tự về tự động sau MANUAL_TIMEOUT_MS.
 *
 * Thư viện: OneWire, DallasTemperature, PubSubClient
 */

#include <Arduino.h>
#include <WiFi.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <PubSubClient.h>

// ---------------- Cấu hình phần cứng ----------------
#define TRIG_PIN   5       // HC-SR04 TRIG
#define ECHO_PIN   18      // HC-SR04 ECHO
#define TEMP_PIN   4       // DS18B20 DQ (có điện trở kéo lên 4,7 kΩ)
#define PUMP_PIN   25      // máy bơm vào: PWM (trên Wokwi là LED xanh dương)
#define VALVE_PIN  26      // van thoát ra: chân IN của module relay
#define PWM_FREQ   5000    // Hz
#define PWM_BITS   8       // độ phân giải PWM: 0..255

// ---------------- Cấu hình mạng ----------------
// Tên mạng WiFi ảo của trình mô phỏng: Wokwi phát "Wokwi-GUEST", Cirkit Designer phát "CirkitWifi".
const char* WIFI_SSID_WOKWI  = "Wokwi-GUEST";
const char* WIFI_SSID_CIRKIT = "CirkitWifi";
const char* WIFI_PASS = "";
const char* MQTT_HOST = "broker.emqx.io";
const uint16_t MQTT_PORT = 1883;
// Topic riêng cho bể nuôi (không trùng mạch khác trên broker công cộng)
const char* TOPIC_DATA = "lab3/aquatank/data";   // ESP32 -> web
const char* TOPIC_CMD  = "lab3/aquatank/cmd";    // web   -> ESP32

// ---------------- Thông số bể và thuật toán ----------------
const float TANK_HEIGHT_CM = 250.0;    // khoảng cách từ cảm biến tới đáy bể
const float LEVEL_DRAIN    = 0.5;      // m, dưới mức này mở van thoát
const float LEVEL_P50_LOW  = 1.0;      // m, vùng bơm 50%: [1,0; 1,5)
const float LEVEL_P50_HIGH = 1.5;
const float LEVEL_P100_HIGH = 2.0;     // m, vùng bơm 100%: [1,5; 2,0]
const uint32_t PUMP50_MS  = 10000;     // bơm 50% trong 10 s
const uint32_t PUMP100_MS = 20000;     // bơm 100% trong 20 s
const uint32_t DRAIN_MS   = 10000;     // mở van thoát trong 10 s
const uint32_t MANUAL_TIMEOUT_MS = 30000; // chế độ thủ công tự hết sau 30 s
const float TEMP_MIN = 24.0;           // °C, ngưỡng nhiệt độ thấp (chỉ cảnh báo trên web)
const float TEMP_MAX = 32.0;           // °C, ngưỡng nhiệt độ cao (chỉ cảnh báo trên web)
const uint32_t READ_PERIOD_MS = 1000;  // chu kỳ đọc cảm biến

// ---------------- Kiểu dữ liệu ----------------
typedef struct { float dist; float level; float temp; } DuLieu;   // gói đầy đủ (qData)
typedef struct { float level; } CanhBao;                          // gói cảnh báo (qAlert)
typedef enum { CMD_PUMP_ON, CMD_PUMP_50, CMD_PUMP_OFF, CMD_VALVE_ON, CMD_VALVE_OFF, CMD_AUTO } Lenh;
typedef enum { A_NONE, A_PUMP50, A_PUMP100, A_DRAIN, A_MANUAL } HanhDong;
typedef struct { uint8_t pump; bool valve; bool manual; uint8_t action; uint8_t left; } TrangThai;
// pump: % công suất (0/50/100), left: số giây còn lại của lượt chạy tự động

// ---------------- Đối tượng FreeRTOS ----------------
QueueHandle_t qData, qAlert, qLenh, qTrangThai;
SemaphoreHandle_t semWiFi, mSerial;

OneWire oneWire(TEMP_PIN);
DallasTemperature tempSensor(&oneWire);
WiFiClient netClient;
PubSubClient mqtt(netClient);

// ---------------- Serial dùng chung (mutex) ----------------
void logLine(const char* fmt, ...) {
  char buf[256];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  if (xSemaphoreTake(mSerial, pdMS_TO_TICKS(50)) == pdTRUE) {
    Serial.println(buf);
    xSemaphoreGive(mSerial);
  }
}

// ---------------- PWM cho máy bơm (chạy được cả lõi ESP32 Arduino 2.x và 3.x) ----------------
void pwmInit() {
#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
  ledcAttach(PUMP_PIN, PWM_FREQ, PWM_BITS);
#else
  ledcSetup(0, PWM_FREQ, PWM_BITS);
  ledcAttachPin(PUMP_PIN, 0);
#endif
}
void pwmWrite(uint8_t percent) {
  uint32_t duty = (uint32_t)percent * 255 / 100;
#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
  ledcWrite(PUMP_PIN, duty);
#else
  ledcWrite(0, duty);
#endif
}

// ---------------- Sự kiện WiFi -> giơ cờ semWiFi ----------------
// Hàm này chạy trong task sự kiện của hệ thống (không phải ngắt), nên dùng xSemaphoreGive thường.
void onWiFiEvent(WiFiEvent_t event) {
  if (event == ARDUINO_EVENT_WIFI_STA_GOT_IP) {
    logLine("[WIFI] Su kien GOT_IP, IP = %s", WiFi.localIP().toString().c_str());
    xSemaphoreGive(semWiFi);
  } else if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) {
    logLine("[WIFI] Su kien mat ket noi WiFi");
  }
}

// ---------------- Nhận lệnh MQTT (chạy bên trong Task truyền) ----------------
void onMqttMessage(char* topic, byte* payload, unsigned int len) {
  char msg[24];
  unsigned int n = len < sizeof(msg) - 1 ? len : sizeof(msg) - 1;
  memcpy(msg, payload, n);
  msg[n] = '\0';

  Lenh cmd;
  if      (strcmp(msg, "pump_on") == 0)   cmd = CMD_PUMP_ON;
  else if (strcmp(msg, "pump_50") == 0)   cmd = CMD_PUMP_50;
  else if (strcmp(msg, "pump_off") == 0)  cmd = CMD_PUMP_OFF;
  else if (strcmp(msg, "valve_on") == 0)  cmd = CMD_VALVE_ON;
  else if (strcmp(msg, "valve_off") == 0) cmd = CMD_VALVE_OFF;
  else if (strcmp(msg, "auto") == 0)      cmd = CMD_AUTO;
  else { logLine("[TRUYEN] Lenh khong hop le: %s", msg); return; }

  // Không chạm vào bơm/van ở đây: chỉ chuyển lệnh cho Task cảnh báo
  xQueueSend(qLenh, &cmd, 0);
  logLine("[TRUYEN] Nhan lenh: %s", msg);
}

// =====================================================================
// TASK ĐỌC DATA - Core 1, ưu tiên 2
// =====================================================================
float doKhoangCachCm() {
  digitalWrite(TRIG_PIN, LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);
  unsigned long us = pulseIn(ECHO_PIN, HIGH, 30000);   // chờ tối đa 30 ms (khoảng 5 m)
  if (us == 0) return NAN;                             // không có tiếng vọng: đo hỏng
  return us * 0.0343f / 2.0f;                          // tốc độ âm thanh 343 m/s, đi và về
}

void TaskDocData(void* pv) {
  TickType_t lastWake = xTaskGetTickCount();
  float lastDist = TANK_HEIGHT_CM;
  for (;;) {
    DuLieu d;
    float dist = doKhoangCachCm();
    if (isnan(dist)) dist = lastDist;                  // đo hỏng thì giữ giá trị trước
    lastDist = dist;
    d.dist = dist;
    d.level = fmaxf(0.0f, (TANK_HEIGHT_CM - dist) / 100.0f);

    // Đọc kết quả của lần đo nhiệt độ trước, rồi ra lệnh đo lần mới (không chờ 750 ms)
    float t = tempSensor.getTempCByIndex(0);
    d.temp = (t == DEVICE_DISCONNECTED_C) ? NAN : t;
    tempSensor.requestTemperatures();

    // 1. Gửi dữ liệu mực nước TRƯỚC: Task cảnh báo (ưu tiên 3) chen ngang xử lý ngay
    CanhBao c = { d.level };
    xQueueSend(qAlert, &c, 0);

    // 2. In Serial (có khóa mSerial)
    logLine("[DOC] khoang cach=%.1f cm | muc nuoc=%.2f m | nhiet do=%.1f C", d.dist, d.level, d.temp);

    // 3. Gửi gói đầy đủ cho Task truyền; đầy (mất mạng) thì bỏ gói cũ nhất, giữ gói mới
    if (xQueueSend(qData, &d, 0) != pdTRUE) {
      DuLieu cu;
      xQueueReceive(qData, &cu, 0);
      xQueueSend(qData, &d, 0);
    }

    // 4. Ngủ đến đúng nhịp 1 s tiếp theo (không trôi chu kỳ)
    vTaskDelayUntil(&lastWake, pdMS_TO_TICKS(READ_PERIOD_MS));
  }
}

// =====================================================================
// TASK CẢNH BÁO - Core 1, ưu tiên 3 (chủ duy nhất của bơm và van)
// =====================================================================
static uint8_t pumpNow = 0;
static bool valveNow = false;

void apDungDauRa(uint8_t pump, bool valve) {
  if (pump > 0 && valve) valve = false;            // luật: không bơm vào và xả ra cùng lúc
  if (pump != pumpNow) { pwmWrite(pump); pumpNow = pump; }
  if (valve != valveNow) { digitalWrite(VALVE_PIN, valve ? HIGH : LOW); valveNow = valve; }
}

// Vùng mực nước: 0 = cần xả, 1 = bình thường thấp, 2 = bơm 50%, 3 = bơm 100%, 4 = trên 2 m
int vungMucNuoc(float level) {
  if (level < LEVEL_DRAIN) return 0;
  if (level < LEVEL_P50_LOW) return 1;
  if (level < LEVEL_P50_HIGH) return 2;
  if (level <= LEVEL_P100_HIGH) return 3;
  return 4;
}

void TaskCanhBao(void* pv) {
  int vungTruoc = -1;                  // vùng của lần đo trước (-1: chưa có)
  uint8_t action = A_NONE;             // lượt tự động đang chạy
  uint32_t actionUntil = 0;
  bool manual = false;
  uint8_t manPump = 0; bool manValve = false;
  uint32_t manualUntil = 0;

  TrangThai st = { 0, false, false, A_NONE, 0 };
  xQueueOverwrite(qTrangThai, &st);               // hộp thư luôn có giá trị ban đầu

  for (;;) {
    CanhBao c;
    bool coDuLieu = xQueueReceive(qAlert, &c, pdMS_TO_TICKS(200)) == pdTRUE;
    uint32_t now = millis();

    // Hết thời gian của lượt đang chạy
    if (action != A_NONE && (int32_t)(now - actionUntil) >= 0) {
      logLine("[CANH BAO] Ket thuc luot %s", action == A_DRAIN ? "xa nuoc" : "bom");
      action = A_NONE;
    }

    if (coDuLieu) {
      int vung = vungMucNuoc(c.level);
      if (vung != vungTruoc) {                    // mực nước VỪA ĐI VÀO một vùng mới
        vungTruoc = vung;
        uint8_t moi = A_NONE; uint32_t dur = 0;
        if (vung == 2) { moi = A_PUMP50;  dur = PUMP50_MS; }
        if (vung == 3) { moi = A_PUMP100; dur = PUMP100_MS; }
        if (vung == 0) { moi = A_DRAIN;   dur = DRAIN_MS; }
        if (moi != A_NONE && !manual) {
          action = moi; actionUntil = now + dur;
          logLine("[CANH BAO] Muc nuoc %.2f m: %s trong %lu s", c.level,
                  moi == A_PUMP50 ? "bom 50%" : moi == A_PUMP100 ? "bom 100%" : "mo van xa",
                  (unsigned long)(dur / 1000));
        } else if (moi == A_NONE && action != A_NONE) {
          // Rời khỏi vùng đang xử lý sang vùng không cần làm gì: dừng lượt đang chạy
          action = A_NONE;
          logLine("[CANH BAO] Muc nuoc %.2f m: dung bom/van", c.level);
        }
      }
    }

    // Lệnh từ web: xem qLenh, không chờ
    Lenh cmd;
    while (xQueueReceive(qLenh, &cmd, 0) == pdTRUE) {
      if (cmd == CMD_AUTO) { manual = false; continue; }
      if (!manual) { manPump = pumpNow; manValve = valveNow; }   // vào thủ công từ trạng thái hiện tại
      manual = true;
      manualUntil = now + MANUAL_TIMEOUT_MS;
      action = A_NONE;                                          // thủ công thay thế lượt tự động
      if (cmd == CMD_PUMP_ON)   { manPump = 100; manValve = false; }
      if (cmd == CMD_PUMP_50)   { manPump = 50;  manValve = false; }
      if (cmd == CMD_PUMP_OFF)  { manPump = 0; }
      if (cmd == CMD_VALVE_ON)  { manValve = true; manPump = 0; }
      if (cmd == CMD_VALVE_OFF) { manValve = false; }
    }
    if (manual && (int32_t)(now - manualUntil) >= 0) {
      manual = false;
      vungTruoc = -1;                                           // về tự động: xét lại vùng hiện tại
    }

    // Quyết định đầu ra
    uint8_t pump = 0; bool valve = false; uint8_t left = 0;
    if (manual) {
      pump = manPump; valve = manValve;
      left = (uint8_t)(((int32_t)(manualUntil - now) + 999) / 1000);
    } else if (action != A_NONE) {
      if (action == A_PUMP50)  pump = 50;
      if (action == A_PUMP100) pump = 100;
      if (action == A_DRAIN)   valve = true;
      left = (uint8_t)(((int32_t)(actionUntil - now) + 999) / 1000);
    }
    apDungDauRa(pump, valve);

    // Ghi đè trạng thái thật vào hộp thư cho Task truyền
    TrangThai moi = { pumpNow, valveNow, manual, (uint8_t)(manual ? A_MANUAL : action), left };
    if (moi.pump != st.pump || moi.valve != st.valve || moi.manual != st.manual ||
        moi.action != st.action || moi.left != st.left) {
      st = moi;
      xQueueOverwrite(qTrangThai, &st);
    }
  }
}

// =====================================================================
// TASK TRUYỀN DATA - Core 0, ưu tiên 1
// =====================================================================
const char* tenHanhDong(uint8_t a) {
  switch (a) {
    case A_PUMP50:  return "pump50";
    case A_PUMP100: return "pump100";
    case A_DRAIN:   return "drain";
    case A_MANUAL:  return "manual";
    default:        return "none";
  }
}

const char* canhBaoNhiet(float t) {
  if (isnan(t)) return "sensor_error";
  if (t < TEMP_MIN) return "temp_low";
  if (t > TEMP_MAX) return "temp_high";
  return "none";
}

bool ketNoiMQTT() {
  char clientId[32];
  snprintf(clientId, sizeof(clientId), "aqua-esp32-%04X", (unsigned)(esp_random() & 0xFFFF));
  if (mqtt.connect(clientId)) {
    mqtt.subscribe(TOPIC_CMD);
    logLine("[TRUYEN] Da ket noi MQTT broker %s", MQTT_HOST);
    return true;
  }
  IPAddress ip;
  bool dns = WiFi.hostByName(MQTT_HOST, ip);
  logLine("[TRUYEN] Ket noi MQTT loi (rc=%d), DNS %s %s, RSSI %d, thu lai sau 2 s",
          mqtt.state(), dns ? "ok" : "loi", dns ? ip.toString().c_str() : "", WiFi.RSSI());
  return false;
}

void TaskTruyenData(void* pv) {
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.onEvent(onWiFiEvent);

  // Quét mạng rồi chọn đúng tên mạng mà trình mô phỏng đang phát
  const char* ssid = WIFI_SSID_WOKWI;
  int n = WiFi.scanNetworks();
  logLine("[WIFI] Quet thay %d mang", n);
  for (int i = 0; i < n; i++) {
    logLine("[WIFI]  - %s (kenh %d)", WiFi.SSID(i).c_str(), WiFi.channel(i));
    if (WiFi.SSID(i) == WIFI_SSID_CIRKIT) ssid = WIFI_SSID_CIRKIT;
  }
  WiFi.scanDelete();
  logLine("[WIFI] Ket noi vao mang: %s", ssid);
  WiFi.begin(ssid, WIFI_PASS);                    // không ép kênh: tự tìm mạng trên mọi kênh

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMqttMessage);
  mqtt.setBufferSize(512);
  mqtt.setSocketTimeout(5);                       // mỗi lần thử kết nối broker tối đa 5 s
  uint8_t loiMqtt = 0;                            // số lần vào broker thất bại liên tiếp
  uint8_t choWifi = 0;                            // số lần chờ WiFi liên tiếp (mỗi lần 2 s)
  DuLieu dCuoi = { 0, 0, NAN };                   // số đo mới nhất đã lấy từ qData
  bool coData = false;
  TrangThai stDaGui = { 255, false, false, 255, 0 };  // trạng thái đã gửi lần trước (255: chưa gửi)

  for (;;) {
    // "Kết nối WiFi, nếu thành công..." : chưa có WiFi thì ngủ chờ cờ semWiFi
    if (WiFi.status() != WL_CONNECTED) {
      logLine("[TRUYEN] Cho WiFi... (status=%d)", WiFi.status());
      xSemaphoreTake(semWiFi, pdMS_TO_TICKS(2000));  // chờ cờ tối đa 2 s rồi tự kiểm tra lại
      if (++choWifi >= 8) {                       // 16 s vẫn chưa có WiFi: kết nối lại từ đầu
        logLine("[WIFI] Thu ket noi lai tu dau");
        WiFi.disconnect();
        vTaskDelay(pdMS_TO_TICKS(500));
        WiFi.begin(ssid, WIFI_PASS);
        choWifi = 0;
      }
      continue;
    }
    choWifi = 0;
    if (!mqtt.connected()) {
      if (!ketNoiMQTT()) {
        if (++loiMqtt >= 3) {                     // 3 lần liền không vào được broker: làm mới WiFi
          logLine("[TRUYEN] Lam moi ket noi WiFi");
          WiFi.disconnect();
          vTaskDelay(pdMS_TO_TICKS(500));
          WiFi.begin(ssid, WIFI_PASS);
          loiMqtt = 0;
        }
        vTaskDelay(pdMS_TO_TICKS(2000));
        continue;
      }
      loiMqtt = 0;
    }

    mqtt.loop();                                  // giữ kết nối và nhận lệnh (gọi onMqttMessage)

    // Chờ dữ liệu tối đa 100 ms để còn quay lại gọi mqtt.loop() đều đặn.
    // Nếu qData đang dồn nhiều gói (mạng chậm), chỉ giữ gói MỚI NHẤT để web thấy số đo hiện tại.
    DuLieu d;
    bool coMoi = xQueueReceive(qData, &d, pdMS_TO_TICKS(100)) == pdTRUE;
    if (coMoi) {
      DuLieu tiep;
      while (xQueueReceive(qData, &tiep, 0) == pdTRUE) d = tiep;
      dCuoi = d;
      coData = true;
    }

    TrangThai st;
    xQueuePeek(qTrangThai, &st, 0);               // xem trạng thái, không lấy ra
    // Trạng thái bơm/van vừa đổi (ví dụ vừa nhận lệnh từ web): gửi NGAY, không đợi số đo kế tiếp
    bool doiTrangThai = coData && (st.pump != stDaGui.pump || st.valve != stDaGui.valve ||
                                   st.manual != stDaGui.manual || st.action != stDaGui.action);
    if (coMoi || doiTrangThai) {
      char json[256];
      char tbuf[12];
      if (isnan(dCuoi.temp)) strcpy(tbuf, "null"); else snprintf(tbuf, sizeof(tbuf), "%.1f", dCuoi.temp);
      snprintf(json, sizeof(json),
               "{\"dist\":%.1f,\"level\":%.2f,\"temp\":%s,\"pump\":%u,\"valve\":%d,"
               "\"mode\":\"%s\",\"action\":\"%s\",\"left\":%u,\"alert\":\"%s\"}",
               dCuoi.dist, dCuoi.level, tbuf, st.pump, st.valve,
               st.manual ? "manual" : "auto", tenHanhDong(st.action), st.left, canhBaoNhiet(dCuoi.temp));
      if (mqtt.publish(TOPIC_DATA, json)) {
        stDaGui = st;
        logLine("[TRUYEN] Gui: %s", json);
      }
    }
  }
}

// =====================================================================
// SETUP
// =====================================================================
void setup() {
  Serial.begin(115200);
  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  pinMode(VALVE_PIN, OUTPUT);
  digitalWrite(VALVE_PIN, LOW);                   // van đóng lúc khởi động
  pwmInit();
  pwmWrite(0);                                    // bơm tắt lúc khởi động

  // Tạo đối tượng giao tiếp TRƯỚC khi tạo task
  qData      = xQueueCreate(5, sizeof(DuLieu));
  qAlert     = xQueueCreate(5, sizeof(CanhBao));
  qLenh      = xQueueCreate(3, sizeof(Lenh));
  qTrangThai = xQueueCreate(1, sizeof(TrangThai));
  semWiFi    = xSemaphoreCreateBinary();
  mSerial    = xSemaphoreCreateMutex();

  tempSensor.begin();
  if (tempSensor.getDeviceCount() == 0) {
    Serial.println("Canh bao: khong tim thay DS18B20, kiem tra day DQ va dien tro 4,7k");
  }
  tempSensor.setWaitForConversion(false);         // đo nhiệt độ không chặn task
  tempSensor.requestTemperatures();
  Serial.println("He thong khoi dong...");

  //                       hàm             tên          stack  tham số  ưu tiên handle  core
  xTaskCreatePinnedToCore(TaskCanhBao,    "CanhBao",   4096,  NULL,    3,      NULL,   1);
  xTaskCreatePinnedToCore(TaskDocData,    "DocData",   4096,  NULL,    2,      NULL,   1);
  xTaskCreatePinnedToCore(TaskTruyenData, "TruyenData",8192,  NULL,    1,      NULL,   0);
}

void loop() {
  vTaskDelete(NULL);   // loopTask không dùng đến, tự xóa để nhường CPU
}
