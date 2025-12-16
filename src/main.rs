use postgres::{Client, NoTls};
use postgres::Error as PostgresError;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::env;
use std::str;

// Внешние крейты
#[macro_use]
extern crate serde_derive;

// Модель данных
#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: Option<i32>,
    name: String,
    email: String,
}

// Константы
// ВАЖНО: env! работает только во время компиляции. Для корректной работы в Dockerfile
// мы используем переменную DATABASE_URL, которую передает Cargo
const DB_URL: &str = env!("DATABASE_URL");
const OK_RESPONSE: &str = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n";
const NOT_FOUND: &str = "HTTP/1.1 404 NOT FOUND\r\n\r\n";
const INTERNAL_ERROR: &str = "HTTP/1.1 500 INTERNAL ERROR\r\n\r\n";
const BAD_REQUEST: &str = "HTTP/1.1 400 BAD REQUEST\r\nContent-Type: text/plain\r\n\r\n";

fn main() {
    // Установка базы данных
    if let Err(e) = set_database() {
        eprintln!("Error setting database: {}", e);
        return;
    }

    let listener = TcpListener::bind("0.0.0.0:8080").unwrap();
    println!("Server listening on port 8080");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(stream);
            }
            Err(e) => {
                eprintln!("Unable to connect: {}", e);
            }
        }
    }
}

// *** ОСНОВНАЯ ФУНКЦИЯ ЧТЕНИЯ/ОБРАБОТКИ HTTP ***
fn handle_client(mut stream: TcpStream) {
    // Используем большой буфер для чтения первой части запроса
    let mut initial_buffer = [0; 4096];

    // Читаем первую часть запроса
    let size = match stream.read(&mut initial_buffer) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Unable to read stream: {}", e);
            return;
        }
    };

    let initial_request_part = str::from_utf8(&initial_buffer[..size]).unwrap_or_default();

    // Находим разделитель заголовков и тела
    let header_end_index = initial_request_part.find("\r\n\r\n").map(|i| i + 4).unwrap_or(size);

    // Получаем заголовки
    let (headers, _) = initial_request_part.split_at(header_end_index);

    let mut full_request = String::from(initial_request_part);

    // 1. Определяем Content-Length
    let content_length: usize = headers
        .lines()
        .find(|line| line.starts_with("Content-Length:"))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);

    // 2. Читаем остаток тела
    let body_already_read_len = size.saturating_sub(header_end_index);
    let bytes_to_read = content_length.saturating_sub(body_already_read_len);

    if bytes_to_read > 0 {
        let mut body_buffer = vec![0; bytes_to_read];

        // read_exact возвращает () (unit type) при успехе,
        // поэтому мы используем .is_ok() и body_buffer.len() для среза.
        if stream.read_exact(&mut body_buffer).is_ok() {
            full_request.push_str(str::from_utf8(&body_buffer[..]).unwrap_or_default());
        }
    }

    // Определяем тип запроса по первой строке
    let first_line = full_request.lines().next().unwrap_or(NOT_FOUND);

    // В обработчики передается ПОЛНЫЙ запрос
    let (status_line, content) = match first_line {
        r if r.starts_with("POST /users") => handle_post_request(&full_request),
        r if r.starts_with("GET /users/") => handle_get_request(&full_request),
        r if r.starts_with("GET /users") => handle_get_all_request(&full_request),
        r if r.starts_with("PUT /users/") => handle_put_request(&full_request),
        r if r.starts_with("DELETE /users/") => handle_delete_request(&full_request),
        _ => (NOT_FOUND.to_string(), "404 not found".to_string()),
    };

    stream.write_all(format!("{}{}", status_line, content).as_bytes()).unwrap_or_default();
}

// *** ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ***

// Извлекает тело запроса и десериализует его в структуру User
fn get_user_request_body(request: &str) -> Result<User, serde_json::Error> {
    let body = request.split("\r\n\r\n").last().unwrap_or_default();

    // Явная аннотация типа 'User' решает ошибку E0282
    let user: User = serde_json::from_str(body)?;
    Ok(user)
}

// Извлекает ID из URL (например, /users/123)
fn get_id(request: &str) -> &str {
    request.split('/').nth(2).unwrap_or_default().split_whitespace().next().unwrap_or_default()
}

// Создает таблицу в БД
fn set_database() -> Result<(), PostgresError> {
    let mut client = Client::connect(DB_URL, NoTls)?;
    client.batch_execute(
        "CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR NOT NULL
        )",
    )?;
    Ok(())
}

// *** ОБРАБОТЧИКИ CRUD ***

// POST: Создание пользователя
fn handle_post_request(request: &str) -> (String, String) {
    let user: User = match get_user_request_body(request) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("JSON Deserialization ERROR: {}", e);
            return (BAD_REQUEST.to_string(), format!("Invalid user data: {}", e).to_string());
        }
    };

    match Client::connect(DB_URL, NoTls) {
        Ok(mut client) => {
            if let Err(e) = client.execute(
                "INSERT INTO users (name, email) VALUES ($1, $2)",
                &[&user.name, &user.email],
            ) {
                eprintln!("DB execution error: {}", e);
                return (INTERNAL_ERROR.to_string(), "DB error".to_string());
            }
            (OK_RESPONSE.to_string(), "User created".to_string())
        }
        Err(e) => {
            eprintln!("DB connection error: {}", e);
            (INTERNAL_ERROR.to_string(), "Internal error".to_string())
        }
    }
}

// GET: Получение одного пользователя по ID
fn handle_get_request(request: &str) -> (String, String) {
    let id = match get_id(request).parse::<i32>() {
        Ok(i) => i,
        Err(_) => return (NOT_FOUND.to_string(), "Invalid ID or ID missing".to_string()),
    };

    match Client::connect(DB_URL, NoTls) {
        Ok(mut client) => {
            match client.query_one("SELECT id, name, email FROM users WHERE id = $1", &[&id]) {
                Ok(row) => {
                    let user = User {
                        id: row.get(0),
                        name: row.get(1),
                        email: row.get(2),
                    };
                    (OK_RESPONSE.to_string(), serde_json::to_string(&user).unwrap_or_default())
                }
                Err(_) => (NOT_FOUND.to_string(), "User not found".to_string()),
            }
        }
        Err(_) => (INTERNAL_ERROR.to_string(), "Internal error".to_string()),
    }
}

// GET: Получение всех пользователей
fn handle_get_all_request(_request: &str) -> (String, String) {
    match Client::connect(DB_URL, NoTls) {
        Ok(mut client) => {
            let mut users = Vec::new();

            match client.query("SELECT id, name, email FROM users", &[]) {
                Ok(rows) => {
                    for row in rows {
                        users.push(User {
                            id: row.get(0),
                            name: row.get(1),
                            email: row.get(2),
                        });
                    }
                    (OK_RESPONSE.to_string(), serde_json::to_string(&users).unwrap_or_default())
                }
                Err(_) => (INTERNAL_ERROR.to_string(), "Error querying users".to_string()),
            }
        }
        Err(_) => (INTERNAL_ERROR.to_string(), "Internal error".to_string()),
    }
}

// PUT: Обновление пользователя
fn handle_put_request(request: &str) -> (String, String) {
    let id = match get_id(request).parse::<i32>() {
        Ok(i) => i,
        Err(_) => return (NOT_FOUND.to_string(), "Invalid ID or ID missing".to_string()),
    };

    let user = match get_user_request_body(request) {
        Ok(u) => u,
        Err(_) => return (BAD_REQUEST.to_string(), "Invalid user data".to_string()),
    };

    match Client::connect(DB_URL, NoTls) {
        Ok(mut client) => {
            let rows_affected = client.execute(
                "UPDATE users SET name = $1, email = $2 WHERE id = $3",
                &[&user.name, &user.email, &id],
            ).unwrap_or(0);

            if rows_affected == 0 {
                return (NOT_FOUND.to_string(), "User not found for update".to_string());
            }

            (OK_RESPONSE.to_string(), "User updated".to_string())
        }
        Err(_) => (INTERNAL_ERROR.to_string(), "Internal error".to_string()),
    }
}

// DELETE: Удаление пользователя
fn handle_delete_request(request: &str) -> (String, String) {
    let id = match get_id(request).parse::<i32>() {
        Ok(i) => i,
        Err(_) => return (NOT_FOUND.to_string(), "Invalid ID or ID missing".to_string()),
    };

    match Client::connect(DB_URL, NoTls) {
        Ok(mut client) => {
            let rows_affected = client.execute("DELETE FROM users WHERE id = $1", &[&id]).unwrap_or(0);

            if rows_affected == 0 {
                return (NOT_FOUND.to_string(), "User not found".to_string());
            }

            (OK_RESPONSE.to_string(), "User deleted".to_string())
        }
        Err(_) => (INTERNAL_ERROR.to_string(), "Internal error".to_string()),
    }
}