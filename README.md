# Relay — Chat App demo gRPC

Ứng dụng chat nhỏ dùng để minh họa bốn kiểu giao tiếp RPC của gRPC. Backend viết bằng Java, giao diện chạy trên trình duyệt. Người dùng, lịch sử tin nhắn và các kết nối đang hoạt động được lưu trong RAM; dữ liệu sẽ mất khi tắt server.

## Bốn kiểu RPC trong ứng dụng

| Kiểu giao tiếp | RPC | Chức năng demo |
|---|---|---|
| Unary | `Login` | Đăng nhập bằng tên hiển thị; một yêu cầu, một phản hồi |
| Server streaming | `GetHistory` | Lấy lịch sử phòng chat; server trả từng tin nhắn qua stream |
| Client streaming | `SendBatch` | Gửi nhiều tin nhắn trong một stream; server trả một kết quả tổng kết |
| Bidirectional streaming | `LiveChat` | Tham gia phòng, gửi và nhận tin nhắn cùng sự kiện đang nhập theo thời gian thực |

Định nghĩa các RPC và message nằm trong [`src/main/proto/chat.proto`](src/main/proto/chat.proto).

## Kiến trúc

```text
Trình duyệt (HTML/CSS/JavaScript)
        │ HTTP và WebSocket
        ▼
ChatGateway (Java, cổng 8080)
        │ gRPC trên HTTP/2, dữ liệu Protobuf
        ▼
ChatServer (Java, cổng 50051)
        └── Lưu người dùng, tin nhắn và kết nối trong RAM
```

Trình duyệt trao đổi với Gateway qua HTTP và WebSocket. Gateway gọi ChatServer bằng gRPC. ChatServer xử lý nghiệp vụ và quản lý dữ liệu trong RAM.

## Yêu cầu

- Java 17 trở lên.
- Maven 3.10.0. Trong workspace này Maven đã được cài tại `.tools/apache-maven-3.10.0`.
- Không cần Node.js để chạy giao diện; Gateway phục vụ trực tiếp các file web.

## Khởi chạy

Mở PowerShell tại thư mục dự án và chạy một lệnh. Lệnh này khởi động cả gRPC server và Gateway:

```powershell
& ".\.tools\apache-maven-3.10.0\bin\mvn.cmd" compile exec:java
```

Sau khi cả hai tiến trình khởi động, mở địa chỉ [http://localhost:8080](http://localhost:8080).

Để dừng ứng dụng, nhấn `Ctrl+C` trong cửa sổ PowerShell đang chạy.

Mặc định gRPC server lắng nghe ở cổng `50051`, Gateway ở cổng `8080`. Có thể cấu hình bằng biến môi trường `GRPC_PORT`, `HTTP_PORT` và `GRPC_HOST`.

## Kịch bản demo

1. Chọn **Unary login**, nhập tên hiển thị rồi đăng nhập.
2. Mở một cửa sổ trình duyệt khác và đăng nhập bằng tên khác.
3. Vào **Live chat** ở cả hai cửa sổ để gửi, nhận tin nhắn và xem sự kiện phòng.
4. Vào **Message batch**, nhập nhiều tin nhắn, mỗi tin trên một dòng, rồi gửi. Server nhận stream và phản hồi số tin đã lưu.
5. Vào **History stream** để yêu cầu lịch sử phòng. Server gửi từng tin nhắn bằng server streaming RPC.

Giao diện có nhật ký hoạt động để theo dõi thời điểm bắt đầu và kết thúc từng RPC.

## Lưu trữ và giới hạn demo

- Dữ liệu chỉ tồn tại trong RAM của ChatServer; khởi động lại server sẽ xóa người dùng và lịch sử.
- Mỗi phòng giữ tối đa 200 tin nhắn gần nhất.
- Mỗi client streaming batch nhận tối đa 100 tin nhắn.
- Tên hiển thị được dùng làm danh tính demo; ứng dụng chưa có mật khẩu hay xác thực production.
- Ứng dụng tập trung minh họa giao tiếp gRPC, chưa có database hoặc lưu file lâu dài.

## Cấu trúc mã nguồn

```text
.
├── pom.xml
├── src/main/proto/chat.proto              # Hợp đồng gRPC/Protobuf
├── src/main/java/demo/chat/
│   ├── ChatServer.java                    # Khởi động gRPC server
│   ├── ChatService.java                   # Cài đặt bốn RPC và lưu RAM
│   └── ChatGateway.java                   # REST/WebSocket bridge cho trình duyệt
└── src/main/resources/web/index.html      # Giao diện Chat App
```
