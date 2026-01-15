import subprocess
import json
import time

def send_rpc_message(process, message):
    message_str = json.dumps(message)
    process.stdin.write((message_str + '\n').encode('utf-8'))
    process.stdin.flush()

def read_rpc_response(process):
    # This is a simplified reader. In a real scenario, you'd need to handle
    # potentially incomplete lines and more robust JSON parsing.
    # For now, we assume the server sends a complete JSON object per line.
    while True:
        line = process.stdout.readline().decode('utf-8').strip()
        if line:
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                # Not a JSON line, might be a log or error message
                print(f"Non-JSON output: {line}")
        else:
            # No more output or process terminated
            break
    return None

def main():
    server_path = "snowflake-mcp/server.py"
    python_executable = "snowflake-mcp/.venv/bin/python"

    print(f"Starting MCP server: {python_executable} {server_path}")
    server_process = subprocess.Popen(
        [python_executable, server_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0
    )

    # Give the server a moment to start up
    time.sleep(2)

    try:
        # 1. Send Initialize Request
        init_message = {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "initialize",
            "params": {
                "protocolVersion": "1.0",
                "capabilities": {},
                "clientInfo": {"name": "gemini-cli", "version": "0.1.0"} # Added version
            }
        }
        print("Sending initialize message...")
        send_rpc_message(server_process, init_message)
        init_response = read_rpc_response(server_process)
        print(f"Initialize Response: {init_response}")

        # After initialization, we should expect an 'initialized' notification from the server.
        # However, the current read_rpc_response only reads one line.
        # For simplicity, I'll proceed assuming initialize worked if the response wasn't an error.

        # 2. Send Tools List Request (as a test to ensure communication works)
        list_tools_message = {
            "jsonrpc": "2.0",
            "id": "2",
            "method": "tools/list",
            "params": {}
        }
        print("\nSending tools/list message...")
        send_rpc_message(server_process, list_tools_message)
        list_tools_response = read_rpc_response(server_process)
        print(f"Tools List Response: {list_tools_response}")

        # Now, attempt to call run_saved_query for connector_usage
        # We need month and year. Let's use current month/year for example.
        # User will need to provide actual month/year for the real query.
        current_month = 1 # January
        current_year = 2026

        run_query_message = {
            "jsonrpc": "2.0",
            "id": "3",
            "method": "tools/call",
            "params": {
                "name": "run_saved_query",
                "arguments": {
                    "query_id": "connector_usage",
                    "params": {
                        "month": current_month,
                        "year": current_year
                    }
                }
            }
        }
        print(f"\nSending run_saved_query for 'connector_usage' (Month: {current_month}, Year: {current_year})...")
        send_rpc_message(server_process, run_query_message)
        run_query_response = read_rpc_response(server_process)
        print(f"Run Query Response: {run_query_response}")

    finally:
        print("\nTerminating server process...")
        server_process.terminate()
        server_process.wait(timeout=5)
        stderr_output = server_process.stderr.read().decode('utf-8')
        if stderr_output:
            print(f"Server Stderr:\n{stderr_output}")

if __name__ == "__main__":
    main()