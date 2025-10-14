import os
import yaml
from datetime import datetime
from typing import Dict, Any

class Config:
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self.load_config()

    def load_config(self):
        """加载配置文件"""
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        
        # 如果配置文件不存在，创建默认配置
        if not os.path.exists(config_path):
            self.create_default_config()
        
        # 读取配置文件
        with open(config_path, 'r', encoding='utf-8') as f:
            self._config = yaml.safe_load(f)
        
        # 创建必要的目录
        self.create_directories()
        
        # 处理动态值
        self.process_dynamic_values()

    def create_default_config(self):
        """创建默认配置文件"""
        default_config = {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'name': 'stock_db',
                'user': 'mengliu',
                'password': 'password'
            },
            'api_tokens': {
                'tushare': ''
            },
            'download': {
                'rate_limits': {
                    'yahoo': 1,
                    'akshare': 2,
                    'tushare': 2
                },
                'retry': {
                    'max_retries': 3,
                    'base_delay': 5
                },
                'batch_size': {
                    'us': 100,
                    'cn': 5
                },
                'date_range': {
                    'start_date': '2020-01-01',
                    'end_date': 'now'
                }
            },
            'web_service': {
                'host': '0.0.0.0',
                'port': 5000,
                'ssl': {
                    'enabled': True,
                    'cert_path': 'cert.pem',
                    'key_path': 'key.pem'
                },
                'cors': {
                    'enabled': True,
                    'origins': ['*']
                }
            },
            'paths': {
                'stock_lists': 'stock_lists',
                'logs': 'logs'
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': 'logs/stock_agent.log'
            }
        }
        
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, default_flow_style=False, allow_unicode=True)

    def create_directories(self):
        """创建必要的目录"""
        directories = [
            self._config['paths']['stock_lists'],
            os.path.dirname(self._config['logging']['file'])
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)

    def process_dynamic_values(self):
        """处理配置中的动态值"""
        # 处理日期
        if self._config['download']['date_range']['end_date'] == 'now':
            self._config['download']['date_range']['end_date'] = datetime.now().strftime('%Y-%m-%d')

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        try:
            value = self._config
            for k in key.split('.'):
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    @property
    def db_url(self) -> str:
        """获取数据库URL"""
        db = self._config['database']
        return f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"

    @property
    def tushare_token(self) -> str:
        """获取Tushare API token"""
        return self._config['api_tokens']['tushare']

    @property
    def rate_limits(self) -> Dict[str, int]:
        """获取API速率限制"""
        return self._config['download']['rate_limits']

    @property
    def batch_sizes(self) -> Dict[str, int]:
        """获取批处理大小"""
        return self._config['download']['batch_size']

    @property
    def retry_config(self) -> Dict[str, int]:
        """获取重试配置"""
        return self._config['download']['retry']

    @property
    def date_range(self) -> Dict[str, str]:
        """获取日期范围"""
        return self._config['download']['date_range']

# 创建全局配置实例
config = Config()
