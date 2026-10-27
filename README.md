# Relay — REST API / native gRPC Chat Lab

Electron desktop, backend Java 17 + Maven + Javalin + Protobuf. Hai máy Windows kết nối qua Radmin VPN. Phòng/tin nhắn trong RAM, file trong thư mục tạm.

## Các lệnh tại thư mục gốc

Mở PowerShell hoặc terminal IntelliJ/VS Code tại thư mục chứa package.json.

| Lệnh | Chức năng |
|---|---|
| `npm install` | Cài dependencies và tải Electron |
| `npm run setup` | Build Java/proto thành backend có đủ dependencies |
| `npm run host` | Chạy backend, đợi REST **và** gRPC cùng phiên sẵn sàng, mở Electron host |
| `npm start` | Chỉ mở Electron; không tự chạy Java/Maven/backend |

Yêu cầu Node **22.12+**, npm; chỉ host cần **JDK 17+**. Build cần JDK có javac. Script tìm JAVA_HOME, PATH và JDK IntelliJ trong `%USERPROFILE%\.jdks`. Không tự cài Java. Lần cài/build đầu cần Internet. Nếu PowerShell chặn npm.ps1, dùng `npm.cmd` thay `npm`; không cần đổi execution policy.

Repo chưa có Maven Wrapper. Setup ưu tiên wrapper nếu có, rồi Maven local/PATH. Nếu Windows chưa có Maven, setup tải Maven **3.9.9** vào .tools, xác minh SHA-512; không cài Maven toàn hệ thống.

### Máy A làm server

Lần đầu:

```powershell
npm install
npm run setup
```

Mỗi lần sử dụng:

```powershell
npm run host
```

Trong Electron nhập tên, mã phòng, bấm **Kết nối** để tạo phòng. Terminal in cổng, localhost và IPv4 theo adapter. Giao diện ghi rõ **Máy này đang làm server**. Chọn IP Radmin hoặc nhập thủ công, bấm **Sao chép thông tin** để chia sẻ IP, hai cổng và mã phòng.

Mặc định REST 8080, gRPC 50051. Đổi trước khi chạy:

```powershell
$env:HTTP_PORT = "18080"
$env:GRPC_PORT = "15051"
npm run host
```

Với npm run host, cổng UI phải trùng backend đã chạy. Để chọn cổng trước khi khởi động Java bằng UI, chạy npm start, chọn **Làm server**, nhập cổng rồi kết nối. Không tự đổi cổng khi cổng bận.

Giữ terminal host mở. Đóng cửa sổ host có xác nhận các client sẽ mất kết nối. Ctrl+C dọn các tiến trình do launcher tạo: yêu cầu Java shutdown qua stdin riêng, chờ dọn file tạm; nếu không phản hồi, chỉ dừng cây PID đã tạo, không kill theo tên Java/Electron. Tắt máy/cưỡng bức kill có thể để lại file trong temp.

### Máy B tham gia

Lần đầu:

```powershell
npm install
```

Các lần sau:

```powershell
npm start
```

Chọn **Tham gia server**, nhập tên riêng, IP Radmin **máy A**, cổng REST/gRPC, đúng mã phòng đã tạo; chọn REST/gRPC rồi kết nối. Client không cần Java hay setup backend.

Cấu hình gần nhất lưu ở connection.json trong Electron userData (thường `%APPDATA%\relay-chat-lab`), có thể sửa. Mỗi instance có user ID riêng, cho mở nhiều instance. Không lưu mật khẩu Radmin.

## Radmin VPN và kiểm tra kết nối

1. Hai máy tự cài Radmin VPN và tham gia cùng **mạng riêng** bên ngoài ứng dụng.
2. Kiểm tra online trong Radmin. Người tạo mạng VPN không mặc nhiên là server chat.
3. Máy A chạy host/tạo phòng, chia sẻ đúng IP adapter Radmin và hai cổng.
4. Máy B nhập thông tin đó. localhost chỉ dùng trên cùng máy; 0.0.0.0 là địa chỉ bind, không phải địa chỉ để kết nối.
5. Trên host cho phép đúng Java/hai cổng **TCP REST và gRPC** qua Windows Firewall trên kết nối VPN; có thể giới hạn remote address về IP Radmin máy B. Không tắt toàn bộ firewall, không cần mở cổng router.

App không cài/cấu hình Radmin, không sửa firewall/mạng. Gợi ý adapter qua tên và InterfaceDescription chứa Radmin/Famatech, **không dựa tiền tố IP**. Nếu không đọc được description, vẫn liệt kê IPv4 và cho nhập thủ công.

Nếu lỗi: kiểm tra IP host (không nhập http:// hoặc :port vào ô IP), trạng thái Radmin, cùng mạng riêng, hai cổng, rule firewall, log Java. Backend bind 0.0.0.0. Từ máy B kiểm tra `Test-NetConnection <IP-máy-A> -Port 8080` và tương tự 50051 (thay cổng nếu đổi).

Thiếu Java: cài JDK 17+, đặt JAVA_HOME/PATH, mở lại terminal. Thiếu dependencies/Electron: npm install. Thiếu bản build hoặc sửa Java/proto: npm run setup. Cổng bận: dừng host cũ hoặc chọn cổng khác. Backend thất bại sẽ báo lỗi và không hiển thị kết nối thành công.

## Kiến trúc và hành vi

```text
Renderer sandbox → contextBridge / IPC giới hạn → Electron main
  ├─ HTTP JSON, polling, multipart, HTTP download → Javalin
  └─ native @grpc/grpc-js → Java DemoRpc
                         ↓
                      DemoStore → RAM / temp files
```

- Hai đường dùng chung logic/phòng/file, chung src/main/proto/chat.proto cho Java và Node. Client REST và gRPC trao đổi dữ liệu cùng phòng.
- REST: POST JSON, GET lịch sử sau cursor, polling mặc định 500 ms (chỉnh 100–10000 ms); request sau chỉ lên lịch khi request trước xong. Upload multipart có file/size/sha256, download HTTP stream.
- gRPC: unary Join/History/Health, bidirectional Chat, client-streaming Upload, server-streaming Download. Tái sử dụng channel, chunk 64 KiB, backpressure, hàng đợi chat giới hạn. History chỉ đồng bộ khi vào phòng, chuyển sang gRPC hoặc kết nối lại; tin mới đến qua stream Chat.
- File .txt tối đa 32 MiB/file, tổng 256 MiB/phiên. Truyền nguyên byte, giữ Unicode/xuống dòng. Metadata ID/tên/size/SHA-256; server kiểm tra đủ byte/checksum trước khi công bố. Không dùng tên client làm đường dẫn lưu. Không Base64, không đọc file lớn toàn bộ vào RAM hay đưa toàn bộ file qua IPC.
- Download ghi .partial, kiểm tra size/hash rồi đổi tên; lỗi xóa file dở. Upload lỗi/hủy không công bố và dọn file dở. File REST tải được qua gRPC và ngược lại.
- Đổi phương thức giữ người dùng/phòng/lịch sử, dừng stream/poll cũ, đồng bộ cursor liên tiếp và loại trùng theo ID. Khóa đổi khi truyền file hoặc xuất số liệu.
- Mất server/VPN: báo mất kết nối, retry giới hạn với backoff 1/2/4 giây, có nút kết nối lại; không tự thành host. Server còn phiên thì đồng bộ lịch sử thiếu; restart thì báo phiên cũ mất và xóa lịch sử cũ. Host cần tạo lại phòng sau restart trước khi client tham gia.
- Giữ 500 tin gần nhất/phòng, báo nếu lịch sử đã hết hạn. Mã phòng là cơ chế demo, chưa có tài khoản/xác thực sản phẩm.
- UI cũ ở / và /ws, các RPC cũ vẫn giữ với dữ liệu legacy riêng; không tham gia hai đường đo desktop.
- Renderer không có Node integration, dùng preload/contextBridge, IPC giới hạn; tin nhắn hiển thị bằng text, không thực thi HTML.

## Đo hiệu năng

Số liệu thật: HTTP requests, RPC calls, message/chunk, lỗi; thời gian gửi đến server ACK; upload đến server nhận/kiểm tra/lưu đủ; download đến client lưu/kiểm tra đủ; MiB/s. Dùng đồng hồ đơn điệu performance.now() trên cùng client; không trừ timestamp hai máy. Health check lúc connect cũng được tính.

Số liệu phiên: ứng dụng lưu các phép đo phát sinh từ tin nhắn, upload và download thật do người dùng thực hiện; không tự tạo user, tin nhắn hoặc file benchmark. Nút xuất CSV ghi các mẫu cục bộ, trung vị/p95, counters và toàn bộ hội thoại client đã quan sát. Mỗi client xuất một CSV để lấy độ trễ thao tác do chính máy đó khởi tạo.

Khi đo VPN, hai phương thức phải dùng cùng kết nối Radmin. Người dùng tự ghi Direct/Relay theo Radmin; app không giả vờ tự phát hiện. VPN, Direct/Relay, tải máy và polling ảnh hưởng kết quả. Protobuf không tự nén file, gRPC không bảo đảm luôn nhanh hơn. Benchmark nhỏ để minh họa, không phải kết luận tổng quát.

## Kiểm thử từ repo

Thử một máy: chạy host/tạo phòng; terminal khác chạy npm start, nhập 127.0.0.1, đúng cổng/phòng và tên khác. Cho phép mở nhiều client.

```powershell
npm test
npm run test:integration
npm run test:desktop
```

- npm test: validation cổng, adapter, cổng bận.
- Integration: nhiều client cục bộ, chat REST↔REST/gRPC↔gRPC/REST↔gRPC, đổi phương thức, checksum bốn tổ hợp file, Unicode, upload bị ngắt, sai phòng, mất server và reconnect/epoch.
- Desktop: Playwright mở hai Electron instance ẩn; host/client, chat chéo, text an toàn, chuyển phương thức, benchmark/CSV, đóng host/giải phóng cổng. Không cần tải browser riêng.
- Kết quả kiểm thử thực tế ở [tests/VERIFICATION.md](tests/VERIFICATION.md).

### Checklist hai máy thật

Chưa kiểm thử hai máy thật qua Radmin; loopback không xác minh đường truyền VPN. Cần kiểm tra thủ công:

- Hai máy khác mạng Internet, cùng Radmin private network; IP và cả hai cổng vào được.
- Sai mã phòng bị từ chối; đúng mã gửi nhận hai chiều ở cả ba tổ hợp phương thức.
- Đổi phương thức không trùng/mất tin; file Unicode/CRLF, file rỗng và gần 32 MiB có cùng checksum ở bốn tổ hợp upload/download.
- Ngắt VPN/host trong chat/truyền file: báo lỗi, không công bố file dở, retry giới hạn; kết nối lại khi cùng phiên và sau restart.
- Đóng host/Ctrl+C giải phóng cổng, không dừng ứng dụng Java khác.
- Benchmark qua cùng VPN, ghi Direct/Relay quan sát thật và lưu CSV.
