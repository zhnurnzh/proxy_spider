import scrapy
import json
import time
import base64

class ProxyCollectorSpider(scrapy.Spider):
    name = "proxy_collector"
    allowed_domains = ["advanced.name"]
    start_urls = ["https://advanced.name/freeproxy"]

    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 1.0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 30.0,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408],
    }

    default_headers = {
        "sec-fetch-mode": "navigate",
        "User-Agent": "Mozilla/5.0",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()
        # now a list of dicts
        self.proxies = []

    def parse(self, response):
        rows = response.xpath("//table//tr")[1:]
        for row in rows:
            if len(self.proxies) >= 150:
                break

            ip_b64    = row.xpath(".//td[2]/@data-ip").get()
            port_b64  = row.xpath(".//td[3]/@data-port").get()
            prot_list = row.xpath(".//td[4]/a/text()").getall()

            if not (ip_b64 and port_b64 and prot_list):
                continue

            try:
                ip   = base64.b64decode(ip_b64).decode().strip()
                port = int(base64.b64decode(port_b64).decode().strip())
            except Exception:
                continue

            
            prots = list({p.strip().upper() for p in prot_list if p.strip()})
            self.proxies.append({
                "ip": ip,
                "port": port,
                "protocols": prots
            })

        
        if len(self.proxies) < 150 and "page=2" not in response.url:
            yield scrapy.Request(
                url="https://advanced.name/freeproxy?page=2",
                headers=self.default_headers,
                callback=self.parse,
            )

    def closed(self, reason):
        
        with open("proxies.json", "w") as f:
            json.dump(self.proxies[:150], f, indent=2)

        elapsed = time.time() - self.start_time
        self.logger.info(f"Scraped {len(self.proxies[:150])} proxies in {elapsed:.1f}s")
