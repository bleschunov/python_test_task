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


"""Символ—разделитель для частей категории товара."""
CATEGORY_PATH_SEP = "/"


@dataclasses.dataclass(frozen=True)
class Category:
    """Класс категории товара на маркетплейсе.

    path — полная категория.
    """

    id: str
    parent_id: str
    name: str
    path: str


@dataclasses.dataclass(frozen=True)
class Offer:
    """Класс товарной позиции на маркетплейсе."""

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
        """Используем __iter__ для создания кортежа: tuple(Offer(...)).
        Потом этот кортеж можно положить в db.execute() для подставновки значений в SQL."""
        for value in self.__dict__.values():
            yield value


def get_raw_categories(xml_file_path: str) -> dict[str, dict]:
    """
    Парсинг категорий из XML файла.

        Parameters:
            xml_file_path (str): Полный путь до XML файла.

        Returns:
            categories (dict[str, dict]): Словарь, где ключ — это ID категории,
            а значение — категория в формате словаря с ключами — значениями:
                id — ID категории (str),
                parent_id — ID родительской категории (str),
                name — название категории (str),
                path — кортеж с названием категории, заготовка под полный путь категории (tuple[str, ...]).

    """
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
    """
    Построение полных категорий path и создание экземпляров Category.

        Parameters:
            categories (dict[str, dict]): Словарь категорий, который получаем из get_raw_categories.

        Returns:
            category_tree (dict[str, Category]): Словарь, где ключ — это ID категории,
                а значение — категория в виде экземпляра класса Category.
    """
    result = {}
    for id, x in categories.items():
        y = x
        while parent := categories.get(y["parent_id"]):
            x["path"] = (parent["name"],) + x["path"]
            y = parent
        result[id] = Category(
            id, x["parent_id"], x["name"], CATEGORY_PATH_SEP.join(x["path"])
        )
    return result


@contextmanager
def get_db():
    """
    Создание подключения к базе и курсора.

        Returns:
            cursors: Курсор psycopg2 для взаимодействия с БД.

        Пример использования:
            with get_db() as cur:
                cur.execute(...)
    """
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
    """
    Создание записи в таблице offer в БД.

        Parameters:
            offer (Offer): Экземпляр класса Offer.
    """
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
    """
    Получение N части категории товара.

        Parameters:
            offer_category_id (Offer): Экземпляр класса Offer.
            level (int): N часть категории товара. Число >=1 или =-1.
            category_tree: Словарь из build_category_tree.

        Returns:
            path (str): N часть категории товара
                Если level=-1, то вернётся вся категория за исключением 1, 2 и 3 части категории.
    """
    offer_category = category_tree.get(str(offer_category_id))
    path = offer_category.path.split(CATEGORY_PATH_SEP)

    if level == -1:
        path.pop(0)
        path.pop(0)
        path.pop(0)
        return CATEGORY_PATH_SEP.join(path)

    return path[level - 1]


# {'name', 'oldprice', 'description', 'param', 'group_id', 'url', 'categoryId', 'vendor', 'currencyId', 'modified_time', 'picture', 'price', 'barcode'}
"""Таблица маппинга тегов из XML на поля в БД.
    
    Встречающиеся варианты тегов в пример XML из задания: 
        {'name', 'oldprice', 'description', 'param', 'group_id', 'url', 
        'categoryId', 'vendor', 'currencyId', 'modified_time', 'picture', 
        'price', 'barcode'}
    
    Получил прохождением по всем тегам в файле и добавлении тегов в set().
    
    vendor похож на brand. Однако в схеме БД по заданию brand — это integer.
    Посчитал, что это — ошибка, и исправил brand integer на brand text в схеме.
"""
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
    """
    Основная функция обработки товарной позиции из XML.
    Раскидывает аттрибуты и текст тега по словарю offer.
    Добавляет теги <param> в словарь params,
    который потом добавляется в offer в виде JSON.

    Создаёт экземпляр Offer из словаря offer и создаёт запись в БД.

        Parameters:
            offer_tag (etree.Element): Очередной тег <offer> из XML.
            category_tree: Словарь из build_category_tree.
    """
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
