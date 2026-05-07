# MEx test http server

A simple http server to mock different http(s) x-systems. 

> GET|POST /v0/http-test-server/<path-to-file>

- path-to-file gets mapped to directory under `HttpTestServerSettings.http_test_server_test_data_directory
- if files are present they are served (ext => mimetype)
- congifuration:
    - use `HttpTestServerSettings.http_test_server_test_data_directory` to define root directory for test data
- Only working for GET and POST, basic idea
    - extractor1
        - file1.json
    - extractor2
        - anotherFile.xml
        - anotherFile1.csv
- if u need custom post logic u might have to write ur own endpoint see `http-test-server/main.py::post_datscha_web_login`

> HEAD /v0/http-test-server/<path-to-file>

- always returns 200 OK

- Post handling could be improved
