"""股票服务Web API"""

import os
from datetime import datetime, timedelta
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
import ssl
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from typing import Dict, Any, Optional, List

import stock_strategy
from config import config
from dao import StockDAO, TaskDAO
from task_manager import task_manager

# 创建Flask应用
app = Flask(__name__)
CORS(app)  # 启用CORS支持

class StockService:
    """股票服务类"""
    def __init__(self):
        """初始化服务"""
        self.strategies = []

    def find_stocks_by_price(
        self,
        market: str,
        days: int,
        price_type: str,
        price_low: float,
        price_high: float
    ) -> List[Dict[str, Any]]:
        """根据价格范围查找股票"""
        # 转换市场代码
        market_code = 'us' if market.lower() == 'us' else 'cn'
        stock_dao = StockDAO(market_code)
        
        # 获取价格数据
        df = stock_dao.get_stock_prices(days)
        if df.empty:
            return []
        
        # 过滤价格范围
        df = df[df[price_type].between(price_low, price_high)]
        if df.empty:
            return []
        
        # 获取最新数据
        latest_prices = df.sort_values('date').groupby('symbol').last().reset_index()
        
        # 转换结果
        stocks = []
        for _, row in latest_prices.iterrows():
            stocks.append({
                'symbol': row['symbol'],
                'date': row['date'].strftime('%Y-%m-%d'),
                'price': float(row[price_type])
            })
        
        return stocks

# 创建服务实例
stock_service = StockService()

@app.route('/api/stocks/price', methods=['GET'])
def find_stocks_by_price():
    """根据价格范围查找股票的API"""
    try:
        # 获取请求参数
        market = request.args.get('market', 'cn')
        days = int(request.args.get('days', 1))
        price_type = request.args.get('price_type', 'close')
        price_low = float(request.args.get('price_low', 0))
        price_high = float(request.args.get('price_high', float('inf')))
        
        # 参数验证
        if market.lower() not in ['us', 'cn']:
            return jsonify({'error': 'Invalid market parameter'}), 400
            
        if price_type.lower() not in ['low', 'high', 'close']:
            return jsonify({'error': 'Invalid price_type parameter'}), 400
            
        if days < 1:
            return jsonify({'error': 'Days must be positive'}), 400
            
        if price_low < 0 or price_high < price_low:
            return jsonify({'error': 'Invalid price range'}), 400
        
        # 调用服务
        stocks = stock_service.find_stocks_by_price(
            market,
            days,
            price_type.lower(),
            price_low,
            price_high
        )
        
        return jsonify({
            'market': market,
            'days': days,
            'price_type': price_type,
            'price_range': {
                'low': price_low,
                'high': price_high
            },
            'count': len(stocks),
            'stocks': stocks
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stocks/strategies', methods=['GET'])
def apply_strategies():
    """应用策略查找股票的API"""
    try:
        # 获取请求参数
        market = request.args.get('market', 'cn')
        strategy = request.args.get('strategy', None)
        days = int(request.args.get('days', 3))
        
        # 参数验证
        if market.lower() not in ['us', 'cn']:
            return jsonify({'error': 'Invalid market parameter'}), 400
        
        # 调用服务
        results = stock_strategy.apply_strategies(market, strategy, days)
        
        # 统计每个策略的结果数量
        summary = {
            strategy_name: len(stocks)
            for strategy_name, stocks in results.items()
        }
        
        return jsonify({
            'market': market,
            'summary': summary,
            'results': results
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tasks/download', methods=['POST'])
def start_download_task():
    """启动数据下载任务"""
    try:
        # 获取请求参数
        market = request.json.get('market', 'cn')
        force = request.json.get('force', False)
        
        # 参数验证
        if market.lower() not in ['us', 'cn']:
            return jsonify({'error': 'Invalid market parameter'}), 400
        
        # 启动任务
        result = task_manager.start_download_task(market, force)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tasks/status', methods=['GET'])
def get_task_status():
    """获取任务状态"""
    try:
        # 获取请求参数
        task_id = request.args.get('task_id', type=int)
        market = request.args.get('market')
        
        # 获取状态
        task_dao = TaskDAO()
        status = task_dao.get_task_status(task_id, market)
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def create_ssl_context():
    """创建SSL上下文"""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    
    # 生成自签名证书
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, u"localhost")
    ])
    
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.utcnow()
    ).not_valid_after(
        datetime.utcnow() + timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
        critical=False,
    ).sign(private_key, hashes.SHA256())
    
    # 保存证书和私钥
    cert_path = "cert.pem"
    key_path = "key.pem"
    
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    
    with open(key_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    
    context.load_cert_chain(cert_path, key_path)
    return context

def main():
    """启动Web服务"""
    web_config = config.get('web_service')
    
    # 如果启用SSL，创建SSL上下文
    ssl_context = create_ssl_context() if web_config['ssl']['enabled'] else None
    
    print(f"Starting Stock Service...")
    protocol = "https" if web_config['ssl']['enabled'] else "http"
    print(f"Server will be available at: {protocol}://{web_config['host']}:{web_config['port']}")
    print("Press Ctrl+C to stop the server")
    
    # Use Flask's development server with optimized settings
    app.run(
        host=web_config['host'],
        port=web_config['port'],
        ssl_context=ssl_context,
        debug=False,  # Disable debug mode for better performance
        threaded=True,  # Enable threading for concurrent requests
        use_reloader=False,  # Disable auto-reloader to avoid conflicts
        processes=1  # Single process to avoid SSL context issues
    )

if __name__ == "__main__":
    main()