import multiprocessing
from stock_service import app, create_ssl_context
from config import config

def run_http_server():
    """运行HTTP服务器"""
    web_config = config.get('web_service')
    print(f"Starting HTTP Service at http://{web_config['host']}:{web_config['http_port']}")
    
    app.run(
        host=web_config['host'],
        port=web_config['http_port'],
        ssl_context=None,
        debug=False,
        threaded=True,
        use_reloader=False
    )

def run_https_server():
    """运行HTTPS服务器"""
    web_config = config.get('web_service')
    if not web_config['ssl']['enabled']:
        print("HTTPS is disabled in config")
        return
        
    ssl_context = create_ssl_context()
    print(f"Starting HTTPS Service at https://{web_config['host']}:{web_config['https_port']}")
    
    app.run(
        host=web_config['host'],
        port=web_config['https_port'],
        ssl_context=ssl_context,
        debug=False,
        threaded=True,
        use_reloader=False
    )

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
