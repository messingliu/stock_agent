import multiprocessing
import signal
import sys
from stock_service import app, create_ssl_context
from config import config
from werkzeug.serving import make_server

def run_http_server():
    """运行HTTP服务器"""
    web_config = config.get('web_service')
    print(f"Starting HTTP Service at http://{web_config['host']}:{web_config['http_port']}")
    
    # 使用Werkzeug的make_server以获得更好的控制和错误处理
    httpd = make_server(
        web_config['host'],
        web_config['http_port'],
        app,
        threaded=True,
        processes=1
    )
    
    # 设置信号处理以优雅关闭
    def signal_handler(sig, frame):
        print("\nShutting down HTTP server...")
        httpd.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()

def run_https_server():
    """运行HTTPS服务器"""
    web_config = config.get('web_service')
    if not web_config['ssl']['enabled']:
        print("HTTPS is disabled in config")
        return
        
    ssl_context = create_ssl_context()
    print(f"Starting HTTPS Service at https://{web_config['host']}:{web_config['https_port']}")
    
    # 使用Werkzeug的make_server，它更好地处理SSL连接和错误
    httpd = make_server(
        web_config['host'],
        web_config['https_port'],
        app,
        threaded=True,
        processes=1,
        ssl_context=ssl_context
    )
    
    # 设置信号处理以优雅关闭
    def signal_handler(sig, frame):
        print("\nShutting down HTTPS server...")
        httpd.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()

def main():
    """启动双服务"""
    # 创建进程
    http_process = multiprocessing.Process(target=run_http_server)
    https_process = multiprocessing.Process(target=run_https_server)
    
    try:
        # 启动进程
        http_process.start()
        https_process.start()
        
        print("Press Ctrl+C to stop all servers")
        
        # 等待进程结束
        http_process.join()
        https_process.join()
        
    except KeyboardInterrupt:
        print("\nShutting down servers...")
        # 优雅地关闭进程
        http_process.terminate()
        https_process.terminate()
        http_process.join()
        https_process.join()
        print("Servers stopped")

if __name__ == "__main__":
    # 确保在主进程中运行
    multiprocessing.freeze_support()
    main()
