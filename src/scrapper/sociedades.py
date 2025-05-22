
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
from config.config_loader import Config
import time

class SociedadesScraper:
    def __init__(self, config: Config, wait_time: int = 5):
        self.config = config
        self.url = config["url"]["url_1"]
        self.wait_time = wait_time
        self.driver = self._init_driver()

    def _init_driver(self):
        options = Options()
        if self.config.get("execute", {}).get("headless", True):
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.binary_location = self.config["tools"]["chromium_path"]

        service = ChromeService(executable_path=self.config["tools"]["chromedriver_path"])
        return webdriver.Chrome(service=service, options=options)

    def load_page(self):
        self.driver.get(self.url)
        self.driver.implicitly_wait(self.wait_time)

    def expand_table_if_needed(self):
        select = Select(self.driver.find_element(By.XPATH, '//select[@name="tblSociedades_length"]'))
        select.select_by_value("-1")
        time.sleep(2)

    def get_table_html(self) -> str:
        table = self.driver.find_element(By.XPATH, '//*[@id="tblSociedades"]')
        return table.get_attribute('outerHTML')

    def parse_html(self, html: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tbody tr")
        return [[td.text.strip() for td in row.find_all("td")] for row in rows if row.find_all("td")]

    def run(self) -> list:
        try:
            self.load_page()
            self.expand_table_if_needed()
            html = self.get_table_html()
            return self.parse_html(html)
        finally:
            self.driver.quit()
