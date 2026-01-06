import threading
import signal
import sys
import traceback
import time
import socket
from stock_service import app, create_ssl_context
from config import config
from werkzeug.serving import make_server

def check_port_available(host, port):
    """检查端口是否可用"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex((host, port))
            return result != 0  # 0 means port is in use
    except Exception as e:
        print(f"Error checking port {port}: {e}")
        return False

def run_http_server():
    """运行HTTP服务器"""
    try:
        web_config = config.get('web_service')
        host = web_config['host']
        port = web_config['http_port']
        
        print(f"[HTTP] Checking if port {port} is available...")
        if not check_port_available(host, port):
            print(f"[HTTP] ERROR: Port {port} is already in use!")
            return
        
        print(f"[HTTP] Starting HTTP Service at http://{host}:{port}")
        
        # 使用Werkzeug的make_server，在单独的线程中运行
        httpd = make_server(
            host,
            port,
            app,
            threaded=True,
            processes=1
        )
        print(f"[HTTP] HTTP server started successfully on port {port}")
        httpd.serve_forever()
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"[HTTP] ERROR: Port {web_config['http_port']} is already in use!")
        else:
            print(f"[HTTP] Error starting HTTP server: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"[HTTP] Error starting HTTP server: {e}")
        traceback.print_exc()

def run_https_server():
    """运行HTTPS服务器"""
    try:
        web_config = config.get('web_service')
        if not web_config['ssl']['enabled']:
            print("[HTTPS] HTTPS is disabled in config")
            return
        
        host = web_config['host']
        port = web_config['https_port']
        
        print(f"[HTTPS] Checking if port {port} is available...")
        if not check_port_available(host, port):
            print(f"[HTTPS] ERROR: Port {port} is already in use!")
            return
        
        print(f"[HTTPS] Creating SSL context...")
        ssl_context = create_ssl_context()
        print(f"[HTTPS] SSL context created successfully")
        
        print(f"[HTTPS] Starting HTTPS Service at https://{host}:{port}")
        
        # 使用Werkzeug的make_server，在单独的线程中运行
        httpd = make_server(
            host,
            port,
            app,
            threaded=True,
            processes=1,
            ssl_context=ssl_context
        )
        print(f"[HTTPS] HTTPS server started successfully on port {port}")
        httpd.serve_forever()
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"[HTTPS] ERROR: Port {web_config['https_port']} is already in use!")
        else:
            print(f"[HTTPS] Error starting HTTPS server: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"[HTTPS] Error starting HTTPS server: {e}")
        traceback.print_exc()

def main():
    """启动双服务"""
    # 使用线程而不是进程，因为Flask app在进程间共享会有问题
    http_thread = threading.Thread(target=run_http_server, name="HTTP-Server", daemon=True)
    https_thread = threading.Thread(target=run_https_server, name="HTTPS-Server", daemon=True)
    
    try:
        # 启动线程
        print("[MAIN] Starting HTTP server thread...")
        http_thread.start()
        
        print("[MAIN] Starting HTTPS server thread...")
        https_thread.start()
        
        # 等待一下，检查线程是否正常启动
        time.sleep(3)
        
        # 检查线程状态
        if not http_thread.is_alive():
            print("[MAIN] ERROR: HTTP server thread died immediately!")
        
        if not https_thread.is_alive():
            print("[MAIN] ERROR: HTTPS server thread died immediately!")
        
        if http_thread.is_alive() and https_thread.is_alive():
            print("[MAIN] Both servers started successfully")
            print("Press Ctrl+C to stop all servers")
        else:
            print("[MAIN] WARNING: One or more servers failed to start")
            return
        
        # 设置信号处理
        def signal_handler(sig, frame):
            print("\n[MAIN] Shutting down servers...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 保持主线程运行
        try:
            while True:
                time.sleep(1)
                if not http_thread.is_alive() or not https_thread.is_alive():
                    print("[MAIN] One of the server threads has stopped")
                    break
        except KeyboardInterrupt:
            print("\n[MAIN] Shutting down servers...")
        
    except Exception as e:
        print(f"[MAIN] Unexpected error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
