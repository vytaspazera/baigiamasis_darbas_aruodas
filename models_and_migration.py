from sqlalchemy import create_engine, ForeignKey, Column, String, Float, Integer, DateTime
from functions_and_classes import MyScrappingBotMainSite
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Dict
import pandas as pd
import os

Base = declarative_base()


class RealEstateListing(Base):
    """_summary_
    Lentelė su unikaliais aruodo listingais
    """
    __tablename__ = "real_estate_listings"  
    url = Column("url", String, primary_key=True)
    status = Column("status", String, default="ongoing")
    apartment_or_house = Column("apartment_or_house", String)
    rent_or_sale = Column("rent_or_sale", String)
    city = Column("city", String)
    district = Column("district", String)
    street_name = Column("street_name", String)
    home_number = Column("home_number", String)
    apartment_number = Column("apartment_number", String)
    year = Column("year", String)
    area = Column("area", Float)
    room_number = Column("room_number", Integer)
    floor_number = Column("floor_number", Integer)
    no_of_floors = Column("no_of_floors", Integer)
    building_type = Column("building_type", String)
    heating_system = Column("heating_system", String)
    indoor_installation = Column("indoor_installation", String)
    energy_consumption_building_class = Column("energy_consumption_building_class", String)
    price = Column("price", Integer)
    date_created = Column('date_created', DateTime)
    latest_date_updated = Column('latest_date_updated', DateTime)
    latest_followed_by_no_people = Column("latest_followed_by_no_people", Integer)
    viewed_by_no_people_overall = Column("viewed_by_no_people_overall", Integer)
    
    def __init__(self, url, status, apartment_or_house, rent_or_sale, city, district, street_name, home_number, apartment_number, year, area, room_number, floor_number, no_of_floors,
                 building_type, heating_system, indoor_installation, energy_consumption_building_class, price, date_created, latest_date_updated, latest_followed_by_no_people, viewed_by_no_people_overall):
        self.url = url
        self.status = status
        self.apartment_or_house = apartment_or_house
        self.rent_or_sale = rent_or_sale
        self.city = city
        self.district = district
        self.street_name = street_name
        self.home_number = home_number
        self.apartment_number = apartment_number
        self.year = year
        self.area = area
        self.room_number = room_number
        self.floor_number = floor_number
        self.no_of_floors = no_of_floors
        self.building_type = building_type
        self.heating_system = heating_system
        self.indoor_installation = indoor_installation
        self.energy_consumption_building_class = energy_consumption_building_class
        self.price = price
        self.date_created = date_created
        self.latest_date_updated = latest_date_updated
        self.latest_followed_by_no_people = latest_followed_by_no_people
        self.viewed_by_no_people_overall = viewed_by_no_people_overall
        
    def __repr__(self):
        return f"({self.url}, {self.status}, {self.apartment_or_house}, {self.rent_or_sale}, {self.city}, {self.district}, {self.street_name}, {self.home_number}, {self.apartment_number}, {self.year}, {self.area}, {self.floor_number}, {self.no_of_floors}, {self.building_type}, {self.heating_system}, {self.indoor_installation}, {self.energy_consumption_building_class}, {self.price}, {self.date_created}, {self.latest_date_updated}, {self.latest_followed_by_no_people}, {self.viewed_by_no_people_overall})"


class RealEstateHistory(Base):
    """_summary_
    Lentelė su aruodo listingų istorijomis. Su RealEstateListing ryšys 1:M per url stulpelius
    """
    __tablename__ = "real_estate_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, ForeignKey("real_estate_listings.url"))
    date_uploaded = Column('date_uploaded', DateTime)
    date_updated = Column('date_updated', DateTime)
    price = Column("price", Float)
    followed_by_no_people = Column("followed_by_no_people", Integer)
    viewed_by_no_people_current_day = Column("viewed_by_no_people_current_day", Integer)
    viewed_by_no_people_overall = Column("viewed_by_no_people_overall", Integer)
    
    def __init__(self, url, date_uploaded, date_updated, price, followed_by_no_people, viewed_by_no_people_current_day, viewed_by_no_people_overall):
        self.url = url
        self.date_uploaded = date_uploaded
        self.date_updated= date_updated
        self.price = price
        self.followed_by_no_people = followed_by_no_people
        self.viewed_by_no_people_current_day = viewed_by_no_people_current_day
        self.viewed_by_no_people_overall = viewed_by_no_people_overall
        
    def __repr__(self):
        return f"({self.url}, {self.date_uploaded}, {self.date_updated}, {self.price}, {self.followed_by_no_people}, {self.viewed_by_no_people_current_day}, {self.viewed_by_no_people_overall})"


engine = create_engine("sqlite:///mydb.db", echo=True)
Base.metadata.create_all(bind=engine)

Session = sessionmaker(bind=engine)
session = Session()


def migrate():
    """_summary_
    Pasiima iš folderio csv prasidedančius result_ ir jų duomenis pagal sąlygas commitin'a į duombazę ir gale pervadina failo pradžia į
    resultdb_ tam kad sekantį kartą neimtų tų pačių duomenų
    """
    all_files = os.listdir('.//01_csv_output_ap_sales//')
    result_files = [f for f in all_files if 'result_' in f]
    for result_file in result_files:
        df = pd.read_csv(f".//01_csv_output_ap_sales//{result_file}")

        for index, row in df.iterrows():
            if session.query(RealEstateHistory).count() == 0 or session.query(RealEstateHistory).filter_by(url=row['url'], date_updated=datetime.strptime(row['date_updated'],'%Y-%m-%d')).first() is None:   
                print(f"Add new listing {row['url']}")
                listing = RealEstateHistory(row['url'],  datetime.strptime(row['date_created'],'%Y-%m-%d'), datetime.strptime(row['date_updated'],'%Y-%m-%d'), row['price'], row['followed_by_no_people'], row['viewed_by_no_people_current_day'], row['viewed_by_no_people_overall'])
                session.add(listing)
                session.commit()
            else:
                print(f"Pass {row['url']}")
                pass
            if session.query(RealEstateListing).count() == 0:
                    # Add new listing
                    listing = RealEstateListing(row['url'], 'ongoing', row['apartment_or_house'], row['rent_or_sale'], row['city'], row['district'], row['street_name'], row['home_number'], row['apartment_number'], row['year'], row['area'], row['room_number'], row['floor_number'], row['no_of_floors'], row['building_type'], row['heating_system'], row['indoor_installation'], row['energy_consumption_building_class'], row['price'], datetime.strptime(row['date_created'],'%Y-%m-%d'), datetime.strptime(row['date_updated'],'%Y-%m-%d'), row['followed_by_no_people'], row['viewed_by_no_people_overall'])
                    session.add(listing)
                    session.commit()
            elif session.query(RealEstateListing.url).filter_by(url=row['url']).first() is None:
                if session.query(RealEstateListing.url).filter_by(status='discontinued', street_name=row['street_name'], room_number=row['room_number'], floor_number=row['floor_number'], no_of_floors=row['no_of_floors'], rent_or_sale=row['rent_or_sale']).first() is not None:
                    # If listing existed but url is new update url
                    delete_old_url = session.query(RealEstateListing).filter_by(status='discontinued', street_name=row['street_name'], room_number=row['room_number'], floor_number=row['floor_number'], no_of_floors=row['no_of_floors'], rent_or_sale=row['rent_or_sale']).one()
                    listings = session.query(RealEstateHistory).filter_by(url=delete_old_url.url).all()
                    for listing in listings:
                        listing.url = row['url']
                        session.commit()
                    session.delete(delete_old_url)
                    session.commit()
                else:
                    pass
                listing = RealEstateListing(row['url'], 'ongoing', row['apartment_or_house'], row['rent_or_sale'], row['city'], row['district'], row['street_name'], row['home_number'], row['apartment_number'], row['year'], row['area'], row['room_number'], row['floor_number'], row['no_of_floors'], row['building_type'], row['heating_system'], row['indoor_installation'], row['energy_consumption_building_class'], row['price'], datetime.strptime(row['date_created'],'%Y-%m-%d'), datetime.strptime(row['date_updated'],'%Y-%m-%d'), row['followed_by_no_people'], row['viewed_by_no_people_overall'])
                session.add(listing)
                session.commit()
            else:
                if datetime.strptime(row['date_updated'],'%Y-%m-%d') > session.query(RealEstateListing.url).with_entities(RealEstateListing.latest_date_updated).filter(RealEstateListing.url == row['url']).first()[0]:
                    #listing updated
                    delete_old_url = session.query(RealEstateListing).filter_by(url=row['url']).one()
                    session.delete(delete_old_url)
                    listing = RealEstateListing(row['url'], 'ongoing', row['apartment_or_house'], row['rent_or_sale'], row['city'], row['district'], row['street_name'], row['home_number'], row['apartment_number'], row['year'], row['area'], row['room_number'], row['floor_number'], row['no_of_floors'], row['building_type'], row['heating_system'], row['indoor_installation'], row['energy_consumption_building_class'], row['price'], datetime.strptime(row['date_created'],'%Y-%m-%d'), datetime.strptime(row['date_updated'],'%Y-%m-%d'), row['followed_by_no_people'], row['viewed_by_no_people_overall'])
                    session.add(listing)
                    session.commit()
                else:
                    pass
        os.rename(f".//01_csv_output_ap_sales//{result_file}", f".//01_csv_output_ap_sales//resultdb_{result_file[7:]}")
        

def update_status(rows_realestatelistings) -> Dict[str, str]:
    """_summary_

    Args:
        rows_realestatelistings : iš duombazės pasiimami url kiekvieno listingo

    Returns:
        dict: url ir status'ai gražinami kaip žodynas, kurie poto bus commitinti į duombazę
    """
    url_and_status = {}
    for row in rows_realestatelistings:
        main_site = MyScrappingBotMainSite(f"https://{row.url}")
        if main_site.ended_listing() is None:
            if row.status == "new" or row.status == "ongoing":
                if row.date_created == row.latest_date_updated:
                    url_and_status[row.url] = 'new'
                else:
                    url_and_status[row.url] = 'ongoing'
            else:
                url_and_status[row.url] = 'recontinued'
        else:
            url_and_status[row.url] = 'discontinued'
    return url_and_status
