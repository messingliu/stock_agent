import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from flask import Flask, request, jsonify
import stock_strategy
from flask_cors import CORS
import ssl

# 加载环境变量
load_dotenv()

# 创建Flask应用
app = Flask(__name__)
CORS(app)  # 启用CORS支持

# 导入配置
from config import config

def get_db_engine():
    """创建PostgreSQL数据库连接"""
    return create_engine(config.db_url)

class StockService:
    def __init__(self):
        self.engine = get_db_engine()
        self.strategies = [
            stock_strategy.GoldenLineDoubleGreenWin()
        ]

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
        table_name = f"{market_code}_stock_prices"
        
        # 构建查询
        query = f"""
            WITH latest_prices AS (
                SELECT DISTINCT ON (symbol) *
                FROM {table_name}
                WHERE date >= CURRENT_DATE - INTERVAL ':days days' AND {price_type} BETWEEN :price_low AND :price_high
                ORDER BY symbol, date DESC
            )
            SELECT DISTINCT p.symbol,
                   i.name,
                   i.exchange,
                   p.date,
                   p.{price_type} as price
            FROM latest_prices p
            LEFT JOIN {market_code}_stocks_info i ON p.symbol = i.symbol
            ORDER BY p.{price_type}
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(query),
                    {
                        'days': days,
                        'price_low': price_low,
                        'price_high': price_high
                    }
                )
                
                stocks = []
                for row in result:
                    stocks.append({
                        'symbol': row.symbol,
                        'name': row.name,
                        'exchange': row.exchange,
                        'date': row.date.strftime('%Y-%m-%d'),
                        'price': float(row.price)
                    })
                return stocks
        except Exception as e:
            print(f"Error finding stocks by price: {str(e)}")
            return []

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
        
        # 参数验证
        if market.lower() not in ['us', 'cn']:
            return jsonify({'error': 'Invalid market parameter'}), 400
        
        # 调用服务
        results = stock_strategy.apply_strategies(market)
        
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

def create_ssl_context():
    """创建SSL上下文"""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    
    # 生成自签名证书
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    import datetime
    
    # 生成私钥
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    
    # 生成证书
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
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=365)
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