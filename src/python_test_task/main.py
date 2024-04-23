import dataclasses
import datetime
import os
import pathlib
from contextlib import contextmanager

import uuid
import json

import psycopg2
import psycopg2.extras
from tqdm import tqdm
from lxml import etree
from dotenv import load_dotenv


@dataclasses.dataclass(frozen=True)
class Category:
    id: str
    parent_id: str
    name: str
    path: str


@dataclasses.dataclass(frozen=True)
class Offer:
    uuid: uuid.UUID
    marketplace_id: int = None
    product_id: int = None
    title: str = None
    description: str = None
    brand: str = None
    seller_id: int = None
    seller_name: str = None
    first_image_url: str = None
    category_id: int = None
    category_lvl_1: str = None
    category_lvl_2: str = None
    category_lvl_3: str = None
    category_remaining: str = None
    features: str = None
    rating_count: int = None
    rating_value: float = None
    price_before_discounts: int = None
    discount: float = None
    price_after_discounts: int = None
    bonuses: int = None
    sales: int = None
    currency: str = None
    barcode: int = None

    def __iter__(self):
        for value in self.__dict__.values():
            yield value


def get_raw_categories(xml_file_path: str) -> dict[str, dict]:
    categories = dict()
    try:
        for action, elem in etree.iterparse(
            xml_file_path, events=("start", "end"), tag=("category", "categories")
        ):
            if action == "end" and elem.tag == "categories":
                break

            if action == "end" and elem.tag == "category":
                categories[elem.attrib.get("id")] = {
                    "parent_id": elem.attrib.get("parentId", ""),
                    "name": elem.text,
                    "path": (elem.text,),
                }
    except etree.XMLSyntaxError as e:
        print(e)
    return categories


def build_category_tree(categories: dict[str, dict]) -> dict[str, Category]:
    result = {}
    for id, x in categories.items():
        y = x
        while parent := categories.get(y["parent_id"]):
            x["path"] = (parent["name"],) + x["path"]
            y = parent
        result[id] = Category(id, x["parent_id"], x["name"], "/".join(x["path"]))
    return result


@contextmanager
def get_db():
    psycopg2.extras.register_uuid()
    con = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
    cur = con.cursor()
    try:
        yield cur
        con.commit()
    except Exception as e:
        print(e)
        con.rollback()
    finally:
        cur.close()
        con.close()


def insert_offer(offer: Offer):
    sql = """
        INSERT INTO sku (
            uuid, marketplace_id, product_id, title, description,
            brand, seller_id, seller_name, first_image_url, category_id,
            category_lvl_1, category_lvl_2, category_lvl_3, category_remaining,
            features, rating_count, rating_value, price_before_discounts,
            discount, price_after_discounts, bonuses, sales, inserted_at,
            updated_at, currency, barcode
        ) 
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, DEFAULT, DEFAULT, %s, %s
        );"""

    with get_db() as cur:
        cur.execute(sql, tuple(offer))


def get_offer_n_level_category(
    offer_category_id: str, level: int, category_tree: dict[str, Category]
) -> str:
    offer_category = category_tree.get(str(offer_category_id))
    path = offer_category.path.split("/")

    if level == -1:
        path.pop(0)
        path.pop(0)
        path.pop(0)
        return "/".join(path)

    return path[level - 1]


# {'name', 'oldprice', 'description', 'param', 'group_id', 'url', 'categoryId', 'vendor', 'currencyId', 'modified_time', 'picture', 'price', 'barcode'}
mapping_table = {
    "name": "title",
    "description": "description",
    "vendor": "brand",
    "picture": "first_image_url",
    "categoryId": "category_id",
    "oldprice": "price_before_discounts",
    "price_after_discounts": "price",
    "inserted_at": datetime.datetime.now(),
    "updated_at": datetime.datetime.now(),
    "currencyId": "currency",
    "barcode": "barcode",
}


def process_offer(offer_tag: etree.Element, category_tree: dict[str, Category]):
    offer = {"uuid": uuid.uuid4(), "product_id": offer_tag.attrib.get("id")}
    params = {}
    for elem in offer_tag:
        if elem.tag in mapping_table:
            offer[mapping_table[elem.tag]] = elem.text
        elif elem.tag == "param":
            params[elem.attrib["name"]] = elem.text

        if elem.tag == "categoryId" and elem.text:
            offer["category_lvl_1"] = get_offer_n_level_category(
                elem.text, 1, category_tree
            )
            offer["category_lvl_2"] = get_offer_n_level_category(
                elem.text, 2, category_tree
            )
            offer["category_lvl_3"] = get_offer_n_level_category(
                elem.text, 3, category_tree
            )
            offer["category_remaining"] = get_offer_n_level_category(
                elem.text, -1, category_tree
            )
    offer["features"] = json.dumps(params, ensure_ascii=False)
    insert_offer(Offer(**offer))


def main():
    load_dotenv()

    xml_path = (pathlib.Path.cwd() / "data" / os.getenv("TARGET_FILENAME")).resolve()
    category_tree = build_category_tree(get_raw_categories(str(xml_path)))

    total_size = os.path.getsize(xml_path)
    with open(xml_path, "rb") as f:
        context = etree.iterparse(f, events=["start"], tag="offer")
        with tqdm(
            total=total_size,
            unit_scale=True,
            bar_format="Processing offers: {l_bar}{bar}{r_bar}",
        ) as pbar:
            for _, offer_tag in context:
                process_offer(offer_tag, category_tree)
                pbar.update(f.tell() - pbar.n)


if __name__ == "__main__":
    main()
