# Kết quả kiểm thử — 26/10/2026

Môi trường: Windows x64, Node 24.11.1, npm 11.6.2, JDK IntelliJ/JBR 21.0.11; biên dịch Java release 17, Maven 3.9.9 local, Electron 43.8.3. Không kiểm thử hai máy thật qua Radmin VPN.

Các lệnh đã chạy:

| Lệnh | Kết quả |
|---|---|
| `npm install` | Cài dependencies, tải Electron thành công |
| `npm run setup` | Maven BUILD SUCCESS, tạo backend đầy đủ dependencies |
| `npm test` | 3/3: validation cổng, adapter IPv4, cổng bận không spawn Java |
| `npm run test:integration` | PASS toàn bộ các kịch bản dưới đây |
| `npm run test:desktop` | PASS hai Electron instance chạy từ source |
| `npm run host` | Cổng mặc định 8080 đang bận: báo lỗi, không mở desktop thành công; chạy với HTTP_PORT=18080 và GRPC_PORT=15051 thành công |
| `npm start` | Mở Electron client, không khởi động Java/backend |

Integration xác minh REST–REST, gRPC–gRPC, REST–gRPC; chuyển phương thức không trùng ID; bốn tổ hợp upload/download giữ nguyên byte, tên Unicode và SHA-256; file rỗng; đồng bộ tin bỏ lỡ khi reconnect cùng phiên; upload gRPC bị ngắt không công bố; từ chối sai mã phòng; phát hiện mất server, restart epoch và reconnect; health check phát hiện riêng lỗi gRPC.

Desktop xác minh tạo host từ UI, client tham gia qua native gRPC, chat hai chiều với REST, HTML người dùng được hiển thị dưới dạng text, renderer không có require/Node integration, chuyển phương thức không trùng tin, xuất CSV từ số liệu phiên thật và 0 lỗi. Đóng host giải phóng backend. Ảnh kiểm thử: target/desktop-smoke.png (không commit).

Thử Ctrl+C ở terminal host 18080/15051: Java ghi log shutdown; kiểm tra bind lại cả hai cổng đều FREE. Tiến trình đang dùng 8080 được giữ nguyên. npm.cmd có thể hỏi `Terminate batch job (Y/N)?` khi nhận Ctrl+C; backend đã được dừng trước câu hỏi này.

Phạm vi chưa xác minh: hai máy khác mạng Internet qua Radmin, Direct/Relay, firewall thực tế, truyền file gần giới hạn 32 MiB và rớt VPN giữa truyền. Checklist thủ công trong README. Không suy diễn kết quả loopback thành kết quả hiệu năng qua VPN.

Chỉ chạy trực tiếp từ repo; không có bước tạo exe hoặc phát hành bộ cài.
