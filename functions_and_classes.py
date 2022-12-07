from selenium import webdriver
import pandas as pd
import warnings
from typing import List
import bs4


class MyScrappingBotMainSite:
    """_summary_
    klasė pasiima linką ir jį su paduotais chromedriverio nustatymais atidaro virtualų driverį su Chromedriver.exe ir
    naudojant klasės metodus gauna reikiamą informaciją su bs4
    """
    def __init__(self, link):
        self.link = link
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
        self.options = webdriver.ChromeOptions()
        self.options.headless = True
        self.options.add_argument(f'user-agent={user_agent}')
        self.options.add_argument(f"--window-size=1920,1080")
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--allow-running-insecure-content')
        self.options.add_argument("--disable-extensions")
        self.options.add_argument("--proxy-server='direct://'")
        self.options.add_argument("--proxy-bypass-list=*")
        self.options.add_argument("--start-maximized")
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--no-sandbox')   
        while True:
            try:
                self.driver = webdriver.Chrome(executable_path="chromedriver.exe", options=self.options)
                self.driver.get(self.link)
                self.bs = bs4.BeautifulSoup(self.driver.page_source, "html.parser")
                break
            except:
                # Naudoju general except, nes error'as kai puslapis tave blokuoja yra labai specifinis ir
                # ir naudojant specifinį except neišna jo pagauti.
                pass

    def get_blocks(self):
        blocks = self.bs.find_all('div', class_='list-adress-v2')
        self.driver.quit()
        return blocks

    def get_last_page(self):
        pagination = self.bs.find('div', class_='pagination')
        self.driver.quit()
        return pagination

    def get_url_data(self):
        url_data_1 = self.bs.find(True, {"class": ["obj-details"]})
        url_data_2 = self.bs.find(True, {"class": ["obj-header-text"]})
        url_data_3 = self.bs.find(True, {"class": ["price-eur"]})
        url_data_4 = self.bs.find(True, {"class": ["obj-stats"]})
        self.driver.quit()
        return url_data_1, url_data_2, url_data_3, url_data_4
    
    def ended_listing(self):
        ended_listing_message = self.bs.find('div', class_='error-div error2')
        self.driver.quit()
        return ended_listing_message
        

def fetch(pages: List[str]) -> pd.DataFrame:
    """_summary_

    Args:
        pages (List[str]): listing'o adresas iš kurio norime išscrapinti duomenis

    Returns:
        pd.DataFrame: pandas dataframe'as su kiekvieno puslapio nuscrapinta reikalinga info
    """
    warnings.filterwarnings(action='ignore', category=FutureWarning)
    df = pd.DataFrame(
        columns=['url',
                 'apartment_or_house',
                 'rent_or_sale',
                 'city',
                 'district',
                 'street_name',
                 'home_number',
                 'apartment_number',
                 'year',
                 'area',
                 'room_number',
                 'floor_number',
                 'no_of_floors',
                 'building_type',
                 'heating_system',
                 'indoor_installation',
                 'energy_consumption_building_class',
                 'price',
                 'date_created',
                 'date_updated',
                 'followed_by_no_people',
                 'viewed_by_no_people_overall',
                 'viewed_by_no_people_current_day'
                 ])

    for page in pages:
        while True:
            try:
                print(f'Working on page" {page}')
                main_site = MyScrappingBotMainSite(page)
                url_data_1, url_data_2, url_data_3, url_data_4 = main_site.get_url_data()

                def site_to_string(text):
                    new_list = list(iter(text.splitlines()))
                    list_clean = [element.replace("NAUDINGA:", "").strip() for element in new_list if element.strip()]
                    string_from_list = ";".join(list_clean)
                    return string_from_list
                cleaned_data = site_to_string(url_data_1.text.strip())
                area = [float(cleaned_data.partition("Plotas:;")[2].split(' m²')[0].replace(",",".")) if "Plotas:;" in cleaned_data else None][0]
                home_number = [cleaned_data.partition("Namo numeris:;")[2].split(';')[0] if "Namo numeris:;" in cleaned_data else None][0]
                apartment_number = [cleaned_data.partition("Buto numeris:;")[2].split(';')[0] if "Buto numeris:;" in cleaned_data else None][0]
                room_number = [int(cleaned_data.partition("Kambarių sk.:;")[2].split(';')[0]) if "Kambarių sk.:;" in cleaned_data else None][0]
                floor_number = [int(cleaned_data.partition("Aukštas:;")[2].split(';')[0]) if "Aukštas:;" in cleaned_data else None][0]
                no_of_floors = [int(cleaned_data.partition("Aukštų sk.:;")[2].split(';')[0]) if "Aukštų sk.:;" in cleaned_data else None][0]
                year = [int(str(cleaned_data.partition("Metai:;")[2].split(';')[0])[:5]) if "Metai:;" in cleaned_data else None][0]
                building_type = [cleaned_data.partition("Pastato tipas:;")[2].split(';')[0] if "Pastato tipas:;" in cleaned_data else None][0]
                heating_system = [cleaned_data.partition("Šildymas:;")[2].split(';')[0] if "Šildymas:;" in cleaned_data else None][0]
                indoor_installation = [cleaned_data.partition("Įrengimas:;")[2].split(';')[0] if "Įrengimas:;" in cleaned_data else None][0]
                energy_consumption_building_class = [cleaned_data.partition("Pastato energijos suvartojimo klasė:;")[2].split(';')[0] if "Pastato energijos suvartojimo klasė:;" in cleaned_data else None][0]
                city = url_data_2.text.split(',')[0].strip()
                district = url_data_2.text.split(',')[1].strip()
                street_name = url_data_2.text.split(',')[2].strip()
                price = int(url_data_3.get_text(strip=True).replace(" ", "").replace("€", ""))
                url = url_data_4.select('dd')[0].get_text(strip=True)
                date_created = url_data_4.select('dd')[1].get_text(strip=True)
                date_updated = url_data_4.select('dd')[2].get_text(strip=True)
                viewed_by_no_people_overall = int(url_data_4.select('dd')[-1].get_text(strip=True).partition("/")[0])
                viewed_by_no_people_current_day = int(url_data_4.select('dd')[-1].get_text(strip=True).partition("/")[2].partition(" ")[0])
                followed_by_no_people = [int(url_data_4.select('dd')[-2].get_text(strip=True)) if url_data_4.select('dt')[-2].get_text(strip=True).partition("/")[0] == "Įsiminė" else 0][0]
                df = df.append({'url': url,
                                'apartment_or_house': 'apartment',
                                'rent_or_sale': 'for sale',
                                'city': city,
                                'district': district,
                                'street_name': street_name,
                                'home_number': home_number,
                                'apartment_number': apartment_number,
                                'year': year,
                                'area': area,
                                'room_number': room_number,
                                'floor_number': floor_number,
                                'no_of_floors': no_of_floors,
                                'building_type': building_type,
                                'heating_system': heating_system,
                                'indoor_installation': indoor_installation,
                                'energy_consumption_building_class': energy_consumption_building_class,
                                'price': price,
                                'date_created': date_created,
                                'date_updated': date_updated,
                                'followed_by_no_people': followed_by_no_people,
                                'viewed_by_no_people_overall': viewed_by_no_people_overall,
                                'viewed_by_no_people_current_day': viewed_by_no_people_current_day
                                }, ignore_index=True)
                break
            except:
                # čia exceptinu visas reklamas kuris įdeda tarp skelbimų
                pass
    return df


def get_links(link_ending: str) -> List[str]:
    """_summary_

    Args:
        link_ending (str): https://www.aruodas.lt/butai/puslapis/ puslapio url pabaigos iš kurių norime gauti visų listingų url

    Returns:
        List[str]: visus to puslapio linkus
    """
    try:
        page_number = int(MyScrappingBotMainSite(f"https://www.aruodas.lt/butai/puslapis/1/{link_ending}").get_last_page().select('a')[-2].get_text(strip=True))
    except AttributeError:
        page_number = 1

    urls = []
    for page in range(1, page_number + 1):
        link_main_site = f"https://www.aruodas.lt/butai/puslapis/{page}/{link_ending}"
        main_site = MyScrappingBotMainSite(link_main_site)
        blocks = main_site.get_blocks()
        for block in blocks:
            try:
                url_aruodas = block.find('a', href=True)['href']
                urls.append(url_aruodas)
            except:
                pass
    return urls
