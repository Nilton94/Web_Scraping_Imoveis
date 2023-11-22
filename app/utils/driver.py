import os
import shutil

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait


logpath = os.path.join(os.getcwd(), 'selenium.log')
chromedriver_path = shutil.which('chromedriver')
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--disable-features=NetworkService")
options.add_argument("--window-size=1920x1080")
options.add_argument("--disable-features=VizDisplayCompositor")
service = Service(
                executable_path = chromedriver_path,
                log_output=logpath,
            )
driver = webdriver.Chrome(options = options, service = service)
