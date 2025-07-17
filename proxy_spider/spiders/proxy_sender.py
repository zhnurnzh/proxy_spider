import os
import json
import scrapy
import time

class SequentialProxySpider(scrapy.Spider):
    name = "proxy_sender"
    allowed_domains = ["test-rg8.ddns.net"]
    start_urls = ["https://test-rg8.ddns.net/task"]
    token = "t_13c7bd3b"

    custom_settings = {
        'DOWNLOAD_DELAY': 10,              
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS': 1,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500,502,503,504,408,429],
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 10,
        'AUTOTHROTTLE_MAX_DELAY': 300,
        'ROBOTSTXT_OBEY': True,
        'HTTPERROR_ALLOWED_CODES': [429],
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.results_path = os.path.join(os.getcwd(), "results.json")
        self.batches = []

    def _load_proxies(self):
        with open("proxies.json") as f:
            raw = json.load(f)
        return [f"{p['ip']}:{p['port']}" for p in raw][:150]

    def start_requests(self):
        proxies = self._load_proxies()
        self.batches = [proxies[i:i+10] for i in range(0, len(proxies), 10)][:15]
        if not self.batches:
            self.logger.error("No proxies to send!")
            return
        yield scrapy.Request(
            self.start_urls[0],
            callback=self.parse_form,
            meta={'batch': 0},
            dont_filter=True
        )

    def parse_form(self, response):
        batch = response.meta['batch']
        if response.status == 429:
            self.logger.warning(f"429 on form batch {batch}, retrying in 30s")
            time.sleep(30)
            return scrapy.Request(
                self.start_urls[0],
                callback=self.parse_form,
                meta={'batch': batch},
                dont_filter=True
            )

        formdata = {'token': self.token}
        for i, proxy in enumerate(self.batches[batch], start=1):
            formdata[f"proxy{i}"] = proxy

        return scrapy.FormRequest.from_response(
            response,
            formdata=formdata,
            callback=self.fetch_token,
            meta={'batch': batch},
            dont_filter=True
        )

    def fetch_token(self, response):
        batch = response.meta['batch']
        if response.status == 429:
            self.logger.warning(f"429 fetching token batch {batch}, retrying form in 30s")
            time.sleep(30)
            return scrapy.Request(
                self.start_urls[0],
                callback=self.parse_form,
                meta={'batch': batch},
                dont_filter=True
            )

        return scrapy.Request(
            "https://test-rg8.ddns.net/api/get_token",
            callback=self.post_proxies,
            meta={'batch': batch},
            dont_filter=True
        )

    def post_proxies(self, response):
        batch = response.meta['batch']
        if response.status == 429:
            self.logger.warning(f"429 on get_token batch {batch}, retrying form in 30s")
            time.sleep(30)
            return scrapy.Request(
                self.start_urls[0],
                callback=self.parse_form,
                meta={'batch': batch},
                dont_filter=True
            )

        payload = {
            "user_id": self.token,
            "len": len(self.batches[batch]),
            "proxies": ", ".join(self.batches[batch])
        }

        return scrapy.Request(
            "https://test-rg8.ddns.net/api/post_proxies",
            method="POST",
            headers={"Content-Type": "application/json"},
            body=json.dumps(payload),
            callback=self.parse_save,
            meta={'batch': batch},
            dont_filter=True
        )

    def parse_save(self, response):
        batch = response.meta['batch']
        if response.status == 429:
            self.logger.warning(f"429 on post_proxies batch {batch}, retrying in 30s")
            time.sleep(30)
            return self.post_proxies(response)

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON in batch {batch}: {response.text}")
            return


        raw_id = data.get("save_id") or data.get("saveId")
        if isinstance(raw_id, str):
            save_id = raw_id
        else:

            save_id = f"batch_{batch}"


        self._save_result(save_id, self.batches[batch])
        self.logger.info(f"Saved batch {batch} → results.json key: {save_id}")
        yield {save_id: self.batches[batch]}


        next_batch = batch + 1
        if next_batch < len(self.batches):
            delay = self.custom_settings['DOWNLOAD_DELAY']
            self.logger.info(f"Sleeping {delay}s before batch {next_batch}")
            time.sleep(delay)
            yield scrapy.Request(
                self.start_urls[0],
                callback=self.parse_form,
                meta={'batch': next_batch},
                dont_filter=True
            )

    def _save_result(self, save_id, proxies):
        try:
            if os.path.exists(self.results_path):
                with open(self.results_path) as f:
                    results = json.load(f)
            else:
                results = {}
            results[save_id] = proxies
            with open(self.results_path, "w") as f:
                json.dump(results, f, indent=4)
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
