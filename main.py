from functions_and_classes import fetch, get_links
from concurrent.futures import ThreadPoolExecutor
from models_and_migration import migrate, update_status, session, RealEstateListing
from datetime import datetime
import pandas as pd
import time


if __name__ == '__main__':
    time1 = time.time()
    """
    1. Iš norimų aruodo linkų paimame duomenis ir patalpiname juos į csv
    """
    # link_endings_aruodas = ['?FAddDate=3&detailed_search=1', '?FChanges=1&FAddDate=3&detailed_search=1'] #3 dienų scrapinimui
    # link_endings_aruodas = ['?FAddDate=1&detailed_search=1', '?FChanges=1&FAddDate=3&detailed_search=1'] #1 dienos scrapinimui
    link_endings_aruodas = ['?FChanges=1&FAddDate=1&detailed_search=1'] #1 dienos tik pakeistos kainos
    for link_ending_aruodas in link_endings_aruodas:
        urls_aruodas = get_links(link_ending_aruodas)
        urls_divided = [[urls_aruodas[i] for i in range(len(urls_aruodas)) if (i % 2) == r] for r in range(2)]
        with ThreadPoolExecutor(16) as executor:
            future0 = executor.submit(fetch, urls_divided[0])
            future1 = executor.submit(fetch, urls_divided[1])
            df0 = future0.result()
            df1 = future1.result()
            df = pd.concat([df0, df1])
        df.to_csv(f".\\01_csv_output_ap_sales\\result_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv", index=False, encoding='utf-8')
    """
    2. Iš result failų migruojame duomenis į duombazę. Dublikuojančius duomenis RealEstateListing lentelėje
    neimportuojame, o kainų ar žmonių peržiūrų pasikeitimus importuojame į RealEstateHistory
    """
    time2 = time.time()
    migrate()
    """
    3. Kai sumigruojame naujus duomenis, norime žinoti status'ą kiekvieno listingo ar jis pasibaigęs ar nepasibaigęs ar
    naujas, todėl kiekvieną listing'ą atidarome ir pažiūrim ar jis vis dar yra
    """
    # rows_realestatelistings = session.query(RealEstateListing).all()
    rows_realestatelistings = session.query(RealEstateListing).filter(RealEstateListing.city == 'Vilnius', RealEstateListing.latest_date_updated > datetime.strptime('2022-12-06','%Y-%m-%d')).all() #demonstracijai
    rows_realestatelistings_divided = [[rows_realestatelistings[i] for i in range(len(rows_realestatelistings)) if (i % 4) == r] for r in range(4)]
    with ThreadPoolExecutor(16) as executor:
        future0 = executor.submit(update_status, rows_realestatelistings_divided[0])
        future1 = executor.submit(update_status, rows_realestatelistings_divided[1])
        future2 = executor.submit(update_status, rows_realestatelistings_divided[2])
        future3 = executor.submit(update_status, rows_realestatelistings_divided[3])
        dict0 = future0.result()
        dict1 = future1.result()
        dict2 = future2.result()
        dict3 = future3.result()
        status_for_db = {**dict0, **dict1, **dict2, **dict3}
    for url, status in status_for_db.items():
        new_status = session.query(RealEstateListing).filter_by(url=url).one()
        new_status.status = status
        session.commit()
        
        
    print(f'Scraping took {round((time2 - time1) // 60)} minutes and {round((time2 - time1) % 60)} seconds.')
    print(f'Status update took {round((time.time() - time2) // 60)} minutes and {round((time.time() - time2) % 60)} seconds.')      
    print(f'Overall it took {round((time.time() - time1) // 60)} minutes and {round((time.time() - time1) % 60)} seconds.')