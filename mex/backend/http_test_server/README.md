# HTTP Test Server

Simple HTTP server for mocking external systems during testing.

## Endpoints

### GET/POST `/v0/http-test-server/{path-to-file}`
- Serves files from `HttpTestServerSettings.http_test_server_test_data_directory`
- Maps `{path-to-file}` to files in the test data directory
- Uses file extension to determine mimetype
- Returns 404 if no matching file or multiple files found

**Example:**
- File at `test-data/http-test-server/extractor1/data.json`
- Served via `/v0/http-test-server/extractor1/data`

### HEAD `/v0/http-test-server/{path-to-file}`
- Always returns HTTP 200 OK status

### POST `/v0/http-test-server/datscha_web/login.php`
- Custom endpoint for datscha web login
- Returns redirect to "verzeichnis.php"

## Directory Structure
```
test-data/http-test-server/
├── extractor1/
│   └── file1.json       # Access via /extractor1/file1
├── extractor2/
│   ├── file.xml        # Access via /extractor2/file
│   └── file.csv        # Access via /extractor2/file
```

## Configuration
- `MEX_HTTP_TEST_SERVER_TEST_DATA_DIRECTORY`: Root directory for test data (default: `test-data/http-test-server`)
- `MEX_HTTP_TEST_SERVER_HOST`: Host (default: `localhost`)
- `MEX_HTTP_TEST_SERVER_PORT`: Port (default: `8088`)
- `MEX_HTTP_TEST_SERVER_ROOT_PATH`: Root path for the server

## Custom Endpoints
Add new endpoints by extending the API router in `main.py`:
```python
@router.post("/http-test-server/custom/path")
def custom_endpoint() -> Response:
    return Response(content="custom response")
