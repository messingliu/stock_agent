"""下载工具类"""

class DownloadStats:
    """下载统计"""
    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = set()
        self.failed_reasons = {}
    
    def add_success(self, count=1):
        """添加成功数量"""
        self.success += count
    
    def add_failure(self, symbols, reason):
        """添加失败记录"""
        if isinstance(symbols, str):
            symbols = [symbols]
        for symbol in symbols:
            self.failed.add(symbol)
            if reason not in self.failed_reasons:
                self.failed_reasons[reason] = set()
            self.failed_reasons[reason].add(symbol)
    
    def print_summary(self):
        """打印统计摘要"""
        print("\nDownload Summary:")
        print(f"Total symbols: {self.total}")
        print(f"Successfully downloaded: {self.success}")
        print(f"Failed downloads: {len(self.failed)}")
        if self.total > 0:
            print(f"Success rate: {(self.success / self.total * 100):.2f}%")
        else:
            print("No symbols downloaded")
        
        if self.failed:
            print("\nFailed symbols by reason:")
            for reason, symbols in self.failed_reasons.items():
                print(f"\n{reason}:")
                print(f"Count: {len(symbols)}")
                print(f"Symbols: {sorted(list(symbols))}")