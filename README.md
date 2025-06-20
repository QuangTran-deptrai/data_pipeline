# data_pipeline

## Mục tiêu

Dự án này xây dựng các pipeline ETL sử dụng Apache Airflow để tự động thu thập, xử lý và đồng bộ dữ liệu từ Circle API vào cơ sở dữ liệu MySQL. Hệ thống phục vụ cho các nghiệp vụ như quản lý thành viên, bài viết, bình luận, sự kiện, thử thách, và bảng xếp hạng trong cộng đồng.



## Kiến trúc & Thành phần chính

- **Apache Airflow**: Quản lý, lập lịch và thực thi các DAG ETL.
- **MySQL**: Lưu trữ dữ liệu đã chuẩn hóa.
- **Circle API**: Nguồn dữ liệu chính (thành viên, post, comment, event...).
- **Docker Compose**: Dùng để khởi tạo môi trường phát triển (Airflow, MySQL...).



## Các DAG chính

- [api_to_db_dag.py](cci:7://file:///c:/Users/QUANG/data_pipel/dags/api_to_db_dag.py:0:0-0:0): Đồng bộ dữ liệu thành viên từ Circle API.
- `api_to_db_dag_post.py`: Đồng bộ bài viết.
- `api_to_db_dag_commments.py`: Đồng bộ bình luận.
- [api_to_db_dag_events.py](cci:7://file:///c:/Users/QUANG/data_pipel/dags/api_to_db_dag_events.py:0:0-0:0): Đồng bộ sự kiện (events) - đã cập nhật schema mới (có trường `ticket_type`...).
- [post_challenges_etl_dag.py](cci:7://file:///c:/Users/QUANG/data_pipel/dags/post_challenges_etl_dag.py:0:0-0:0): Sinh thử thách cho post và event.
- [ranking_etl_dag.py](cci:7://file:///c:/Users/QUANG/data_pipel/dags/ranking_etl_dag.py:0:0-0:0): Tính toán và cập nhật bảng xếp hạng thành viên.



## Hướng dẫn sử dụng

### 1. Cài đặt môi trường

- Clone repo:

  git clone [https://github.com/QuangTran-deptrai/data_pipeline.git](https://github.com/QuangTran-deptrai/data_pipeline.git)
  cd data_pipeline
### 2. Thiết lập biến môi trường & kết nối

- Tạo file `.env` trong thư mục gốc và thêm biến:
    CIRCLE_API_KEY=<your_circle_api_key>
- Đảm bảo Airflow đã có Connection ID tên `QUANG` trỏ tới MySQL (có thể cấu hình qua Airflow UI hoặc CLI).

### 3. Chạy các DAG

- Truy cập Airflow UI tại [http://localhost:8080](http://localhost:8080)
- Bật (enable) các DAG bạn muốn chạy tự động.
- Các DAG sẽ tự động ETL dữ liệu từ Circle API vào MySQL theo lịch trình (mặc định mỗi 30 phút).



## Lưu ý về schema

- **events**: Đã cập nhật các trường mới như `ticket_type`, `status`, `max_attendees`, v.v. và xử lý khóa ngoại với `community_member_id`.
- **post_challenges** và **ranking**: Không còn sử dụng AUTO_INCREMENT, các ID được sinh thủ công trong DAG.


## Đóng góp & Phát triển

- Fork repo, tạo branch mới, commit & pull request để đóng góp.
- Mọi ý kiến về logic ETL, tối ưu schema, hoặc thêm DAG mới đều được chào đón!
- Khi đóng góp, hãy đảm bảo code đã được kiểm thử và tuân thủ cấu trúc dự án.



## Liên hệ

- Tác giả: Quang Tran
- Github: [https://github.com/QuangTran-deptrai](https://github.com/QuangTran-deptrai)

