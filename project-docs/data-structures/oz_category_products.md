# Техническое задание для обновления импорта

The existing import page needs to be updated to add a new import feature. There is a folder containing .xlsx files, and for each file, data must be extracted from the specified sheets and loaded into the corresponding new database tables. Since each table is newly created and all previous data will be deleted before each import, a separate migration is not required.

## Ozon Category Products (Folder) - Data Loading Instructions

The user specifies in the application settings the path to a folder containing .xlsx files. All files in this folder are loaded into the DuckDB table (oz_category_products), and the data from each report is combined (unioned) together.

* **Target DuckDB Table:** oz_category_products
* **File Pattern:** Pattern to identify files to process, e.g., "*.xlsx" in folder
* **Sheet Name:** "Шаблон"
* **Header Row Number:** 2
* **Data Starts on Row:** 4
* **Columns to Load with Data Type Conversions/Transformations:** (List the exact Excel column header names and the corresponding target DuckDB table column names):

TABLE TITLE | TYPE | -- TITLE IN FILE
* oz_vendor_code VARCHAR,                 -- Артикул*
* product_name VARCHAR,                   -- Название товара
* oz_actual_price NUMERIC,                -- Цена, руб.*
* oz_price_before_discount NUMERIC,       -- Цена до скидки, руб.
* vat_percent INTEGER,                    -- НДС, %*
* installment VARCHAR,                    -- Рассрочка
* review_points INTEGER,                  -- Баллы за отзывы
* oz_sku VARCHAR,                         -- SKU
* barcode VARCHAR,                        -- Штрихкод (Серийный номер / EAN)
* package_weight_g INTEGER,               -- Вес в упаковке, г*
* package_width_mm INTEGER,               -- Ширина упаковки, мм*
* package_height_mm INTEGER,              -- Высота упаковки, мм*
* package_length_mm INTEGER,              -- Длина упаковки, мм*
* main_photo_url VARCHAR,                 -- Ссылка на главное фото*
* additional_photos_urls TEXT,            -- Ссылки на дополнительные фото
* photo_360_urls TEXT,                    -- Ссылки на фото 360
* photo_article VARCHAR,                  -- Артикул фото
* oz_brand VARCHAR,                       -- Бренд в одежде и обуви*
* merge_on_card VARCHAR,                  -- Объединить на одной карточке*
* color VARCHAR,                          -- Цвет товара*
* russian_size VARCHAR,                   -- Российский размер*
* color_name VARCHAR,                     -- Название цвета
* manufacturer_size VARCHAR,              -- Размер производителя
* type VARCHAR,                           -- Тип*
* gender VARCHAR,                         -- Пол*
* season VARCHAR,                         -- Сезон
* is_18plus VARCHAR,                      -- Признак 18+
* group_name VARCHAR,                     -- Название группы
* hashtags TEXT,                          -- #Хештеги
* annotation TEXT,                        -- Аннотация
* rich_content_json TEXT,                 -- Rich-контент JSON
* keywords TEXT,                          -- Ключевые слова
* country_of_origin VARCHAR,              -- Страна-изготовитель
* material VARCHAR,                       -- Материал
* upper_material VARCHAR,                 -- Материал верха
* lining_material VARCHAR,                -- Материал подкладки обуви
* insole_material VARCHAR,                -- Материал стельки
* outsole_material VARCHAR,               -- Материал подошвы обуви
* collection VARCHAR,                     -- Коллекция
* style VARCHAR,                          -- Стиль
* temperature_mode VARCHAR,               -- Температурный режим
* foot_length_cm NUMERIC,                 -- Длина стопы, см
* insole_length_cm NUMERIC,               -- Длина стельки, см
* fullness VARCHAR,                       -- Полнота
* heel_height_cm NUMERIC,                 -- Высота каблука, см
* sole_height_cm NUMERIC,                 -- Высота подошвы, см
* bootleg_height_cm NUMERIC,              -- Высота голенища, см
* size_info TEXT,                         -- Информация о размерах
* fastener_type VARCHAR,                  -- Вид застёжки
* heel_type VARCHAR,                      -- Вид каблука
* model_features TEXT,                    -- Особенности модели
* decorative_elements TEXT,               -- Декоративные элементы
* fit VARCHAR,                            -- Посадка
* size_table_json TEXT,                   -- Таблица размеров JSON
* warranty_period VARCHAR,                -- Гарантийный срок
* sport_purpose VARCHAR,                  -- Спортивное назначение
* orthopedic VARCHAR,                     -- Ортопедический
* waterproof VARCHAR,                     -- Непромокаемые
* brand_country VARCHAR,                  -- Страна бренда
* pronation_type VARCHAR,                 -- Тип пронации
* membrane_material_type VARCHAR,         -- Тип мембранного материала
* target_audience VARCHAR,                -- Целевая аудитория
* package_count INTEGER,                  -- Количество заводских упаковок
* tnved_codes VARCHAR,                    -- ТН ВЭД коды ЕАЭС
* platform_height_cm NUMERIC,             -- Высота платформы, см
* boots_model VARCHAR,                    -- Модель ботинок
* shoes_model VARCHAR,                    -- Модель туфель
* ballet_flats_model VARCHAR,             -- Модель балеток
* shoes_in_pack_count INTEGER,            -- Количество пар обуви в упаковке
* error TEXT,                             -- Ошибка
* warning TEXT                            -- Предупреждение
* **Pre-Update Action:** Delete all existing data before inserting new data
* **File Grouping:** Union all records from multiple files

## Пример содержимого листа "Шаблон":
| Название | Цены<br>Заполняйте только при создании нового товара. Изменить цену существующего товара можно в разделе «Цены и акции → Обновить цены» |  |  | Платное продвижение<br>Самые популярные способы поднять конверсию |  |  | Информация о товаре<br>Блок можно не заполнять, если товар продается на Ozon и вы заполнили поле SKU | Дополнительная информация о товаре<br>Блок можно не заполнять, если товар продается на Ozon и вы заполнили либо поле SKU, либо блок ""Информация о товаре"" |  |  |  |  |  |  |  | Одинаковые характеристики<br>Для всех вариантов товара, которые вы хотите объединить на одной карточке, укажите одинаковые значения в этих полях |  | Характеристики вариантов<br>Вы можете заполнить эти поля как одинаковыми значениями (если в этой характеристике вариант не отличается от других) или разными (если отличается) |  |  |  | Характеристики<br>Блок можно не заполнять, если товар продается на Ozon и вы заполнили либо поле SKU, либо блок ""Информация о товаре"" |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| № | Артикул* | Название товара | Цена, руб.* | Цена до скидки, руб. | НДС, %* | Рассрочка | Баллы за отзывы | SKU | Штрихкод (Серийный номер / EAN) | Вес в упаковке, г* | Ширина упаковки, мм* | Высота упаковки, мм* | Длина упаковки, мм* | Ссылка на главное фото* | Ссылки на дополнительные фото | Ссылки на фото 360 | Артикул фото | Бренд в одежде и обуви* | Объединить на одной карточке* | Цвет товара* | Российский размер* | Название цвета | Размер производителя | Тип* | Пол* | Сезон | Признак 18+ | Название группы | #Хештеги | Аннотация | Rich-контент JSON | Ключевые слова | Страна-изготовитель | Материал | Материал верха | Материал подкладки обуви | Материал стельки | Материал подошвы обуви | Коллекция | Стиль | Температурный режим | Длина стопы, см | Длина стельки, см | Полнота | Высота каблука, см | Высота подошвы, см | Высота голенища, см | Информация о размерах | Вид застёжки | Вид каблука | Особенности модели | Декоративные элементы | Посадка | Таблица размеров JSON | Гарантийный срок | Спортивное назначение | Ортопедический | Непромокаемые | Страна бренда | Тип пронации | Тип мембранного материала | Целевая аудитория | Количество заводских упаковок | ТН ВЭД коды ЕАЭС | Высота платформы, см | Модель ботинок | Модель туфель | Модель балеток | Количество пар обуви в упаковке | Ошибка | Предупреждение |
|  | Обязательное поле |  | Обязательное поле |  | Обязательное поле |  |  |  |  | Обязательное поле | Обязательное поле | Обязательное поле | Обязательное поле | Обязательное поле |  |  |  | Обязательное поле<br>ϟ  Влияет на продвижение | Обязательное поле | Обязательное поле<br>Ⓜ️ Множественный выбор | Обязательное поле<br>Ⓜ️ Множественный выбор |  |  | Обязательное поле | Обязательное поле<br>Ⓜ️ Не более 2 вариантов | Рекомендуем к заполнению |  |  |  |  |  |  | Ⓜ️ Множественный выбор | Ⓜ️ Не более 4 вариантов |  | Ⓜ️ Множественный выбор |  | Ⓜ️ Множественный выбор |  | Ⓜ️ Не более 1 вариантов |  |  | Ⓜ️ Не более 6 вариантов |  |  |  |  |  | Ⓜ️ Множественный выбор |  | Ⓜ️ Не более 3 вариантов | Ⓜ️ Не более 3 вариантов |  |  |  | Ⓜ️ Не более 6 вариантов |  |  |  |  |  | Ⓜ️ Множественный выбор |  |  |  |  |  |  |  |  |  |
| 1 | 11100626-белый-38 | Сабо на каблуке кожаные с открытой пяткой и квадратным носом, мюли | 1679 | 3358 | 20 | Нет | Нет |  | 4620143410670 | 500 | 162 | 100 | 300 | https://cdn1.ozone.ru/s3/multimedia-y/6730654030.jpg | https://cdn1.ozone.ru/s3/multimedia-z/6414102455.jpg<br>https://cdn1.ozone.ru/s3/multimedia-c/6414102936.jpg<br>https://cdn1.ozone.ru/s3/multimedia-e/6414102470.jpg<br>https://cdn1.ozone.ru/s3/multimedia-w/6414102452.jpg<br>https://cdn1.ozone.ru/s3/multimedia-y/6414102454.jpg |  | 11100626 | Obba | A11100626 | белый | 38 | Белоснежный 23 | 38 | Сабо | Женский | Лето |  |  |  | Женские мюли на низком каблуке добавят в ваши летние образы женственности. Стильные босоножки на каблуке с квадратным мысом подойдут для широкой, средней и узкой стопы, а дутая перемычка не будет натирать ногу. Женственные сабо на каблучке имеют несколько особенностей. Открытая пятка и нос смотрится утонченно и изящно. А широкий квадратный мыс подчеркнет красивый педикюр. Квадратный низкий каблук необычной формы не шатается, поэтому вам будет комфортно, а небольшое расширение книзу добавит устойчивости. Босоножки сделаны из мягкой экокожи, поэтому стопы не скользят и дышат. Кожаные мюли не теряют вид и имеют высокий уровень износоустойчивости. Верх плотно прилегает к стопе, не натирая ее. Несмотря на каблук, мюли имеют удобную колодку, поэтому вы не будете ощущать дискомфорт. Стильные сабо идеально подойдут для создания невероятно красивых вечерних образов. Вы можете сочетать такие туфли в свадебных, выпускных нарядах. Классические пастельные расцветки идеально впишутся в базовый гардероб. Такие варианты отлично подойдут для деловых встреч, походов в офис на работу, а также для прогулок в выходной день, отдыха на пляже и море. Надевайте модные мюли с открытой пяткой с юбками и брюками. Сочетайте их с расслабленными джинсами или деловыми костюмами, чтобы выглядеть эффектно в любой одежде. Не забудьте дополнить весенний образ стильными украшениями и модной сумкой. Бренд Обба предлагает вам окунуться в мир модной одежды и обуви. В нашем каталоге вы сможете найти больше актуальных моделей обуви для женщин и девушек. Чтобы вам было комфортно, бренд Obba ежедневно работает над созданием удобной обуви. Идеальные колодки фабричного качества адаптированы под анатомические особенности стопы. Они не только сядут на ножку как влитые, но и подарят чувство лёгкости и комфорта. В наших коллекциях вы сможете найти яркие модели, которые сочетают в себе все современные тенденции моды, а также собрать базовый капсульный гардероб. Мы уверены, что красота есть в каждой из вас, независимо от фигуры и комплекции, цвета волос и кожи. В нашем каталоге вы сможете ознакомиться со всем ассортиментом осенней, весенней и летней обуви для женщин. |  | сабо;мюли;босоножки с квадратным носом | Китай (Гонконг) | Экокожа | Искусственная кожа |  | Экокожа | Резина | Весна-лето 2024 | Вечерний/праздничный | от +18 | 24,5 | 25,5 | F | 7 | 1 |  |  | Без застёжки |  | Квадратный нос |  | Нормальная |  |  | Танцы | Да | Нет | Италия | Нейтральная |  | Взрослая | 1 | 6402993100 - МАРКИРОВКА РФ - Обувь прочая, с верхом из пластмассы, с союзкой из ремешков или имеющая одну или несколько перфораций, с подошвой и каблуком высотой более 3 см |  |  |  |  |  |  |  |
| 2 | 1632000118-серый-40 | Сабо | 2699 | 5398 | 20 | Нет | Нет |  | 4815550995951 | 500 | 240 | 130 | 300 | https://cdn1.ozone.ru/s3/multimedia-1-w/6934142084.jpg | https://cdn1.ozone.ru/s3/multimedia-1-5/6933853193.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-q/6933853178.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-r/6934142079.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-1/6930700741.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-0/6930700740.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-4/6930700744.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-2/6930700742.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-y/6930700738.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-3/6930700743.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-x/6930700737.jpg |  |  | X-Plode | 145882463 | серый | 40 | серый; 42 | 40 | Сабо | Мужской | На любой сезон |  |  |  |  | {<br>  ""content"": [<br>    {<br>      ""widgetName"": ""raShowcase"",<br>      ""type"": ""roll"",<br>      ""blocks"": [<br>        {<br>          ""imgLink"": """",<br>          ""img"": {<br>            ""src"": ""https://cdn1.ozone.ru/s3/multimedia-1-l/7369801185.jpg"",<br>            ""srcMobile"": ""https://cdn1.ozone.ru/s3/multimedia-1-l/7369801185.jpg"",<br>            ""alt"": """",<br>            ""position"": ""width_full"",<br>            ""positionMobile"": ""width_full"",<br>            ""widthMobile"": 1500,<br>            ""heightMobile"": 938<br>          }<br>        }<br>      ]<br>    }<br>  ],<br>  ""version"": 0.3<br>} |  | Китай (Гонконг) | EVA | Полимерные материалы | Без подклада |  | EVA | Весна-лето 2025 |  | от + 10 | 25,5 | 26,5 | F |  |  |  |  | Без застёжки |  | Легкая модель;Низкая модель |  | Нормальная |  |  | Спортивная ходьба | Да | Да | Италия | Нейтральная |  | Взрослая | 1 | 6402993900 - МАРКИРОВКА РФ - Прочая обувь, с верхом из пластмассы, с союзкой из ремешков или имеющая одну или несколько перфораций |  |  |  |  |  |  |  |
| 3 | 1632000124-синий-38 | Сабо | 680 | 1360 | 20 | Нет | Нет |  | 4815550986249 | 500 | 230 | 120 | 300 | https://cdn1.ozone.ru/s3/multimedia-1-r/6940910547.jpg | https://cdn1.ozone.ru/s3/multimedia-1-9/6934158801.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-6/6930694446.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-p/6930694789.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-s/6930694792.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-q/6930694754.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-l/6930694713.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-t/6930694721.jpg<br>https://cdn1.ozone.ru/s3/multimedia-1-w/6930694724.jpg |  |  | X-Plode | 145882463 | синий;голубой;белый | 38 | синий;голубой;белый; 1 | 38 | Сабо | Мужской | На любой сезон |  |  |  |  | {<br>  ""content"": [<br>    {<br>      ""widgetName"": ""raShowcase"",<br>      ""type"": ""roll"",<br>      ""blocks"": [<br>        {<br>          ""imgLink"": """",<br>          ""img"": {<br>            ""src"": ""https://cdn1.ozone.ru/s3/multimedia-1-l/7369801185.jpg"",<br>            ""srcMobile"": ""https://cdn1.ozone.ru/s3/multimedia-1-l/7369801185.jpg"",<br>            ""alt"": """",<br>            ""position"": ""width_full"",<br>            ""positionMobile"": ""width_full"",<br>            ""widthMobile"": 1500,<br>            ""heightMobile"": 938<br>          }<br>        }<br>      ]<br>    }<br>  ],<br>  ""version"": 0.3<br>} |  | Китай (Гонконг) | EVA | Полимерные материалы | Без подклада |  | EVA | Весна-лето 2025 |  | от + 10 | 24,5 | 25,5 | F |  |  |  |  | Без застёжки |  | Легкая модель;Низкая модель |  | Нормальная |  |  | Спортивная ходьба | Да | Да | Италия | Нейтральная |  | Взрослая | 1 | 6402993900 - МАРКИРОВКА РФ - Прочая обувь, с верхом из пластмассы, с союзкой из ремешков или имеющая одну или несколько перфораций |

## Ozon Video Products (Folder) - Data Loading Instructions

The user specifies in the application settings the path to a folder containing .xlsx files. All files in this folder are loaded into the DuckDB table (oz_video_products), and the data from each report is combined (unioned) together.

* **Target DuckDB Table:** oz_video_products
* **File Pattern:** Pattern to identify files to process, e.g., "*.xlsx" in folder
* **Sheet Name:** "Озон.Видео"
* **Header Row Number:** 2
* **Data Starts on Row:** 4
* **Columns to Load with Data Type Conversions/Transformations:** (List the exact Excel column header names and the corresponding target DuckDB table column names):

TABLE TITLE | TYPE | -- TITLE IN FILE
* oz_vendor_code VARCHAR,                 -- Артикул*
* video_name VARCHAR,                     -- Озон.Видео: название
* video_link TEXT,                        -- Озон.Видео: ссылка
* products_on_video TEXT,                 -- Озон.Видео: товары на видео
* **Pre-Update Action:** Delete all existing data before inserting new data
* **File Grouping:** Union all records from multiple files

## Пример содержимого листа "Озон.Видео":
| Озон.Видео |  |  |
|---|---|---|
| Артикул* | Озон.Видео: название | Озон.Видео: ссылка | Озон.Видео: товары на видео |
| Обязательное поле |  |  |  |
| 1632000118-серый-40 | 75346ba0041b4d0c93601b9738804577 | https://cdnvideo.v.ozone.ru/vod/video-52/01J6F3ASC0S146HY9CYC6K8FY8/asset_3_h264.mp4 |  |
| 1632000124-синий-38 | 7426d6b97f5f4d06b459296dfc0f210d | https://cdnvideo.v.ozone.ru/vod/video-53/01J87HXPHGQMWJX04C2092Y8FN/asset_2_h264.mp4 |  |
| 0948000050-серый-41 | 259cce8c5f614ebc8a5043a14da6c53f | https://cdnvideo.v.ozone.ru/vod/video-43/01HXGJYN2GB3J2Q74XJGBX3TCF/asset_2_h264.mp4 |


## Ozon Video Cover Products (Folder) - Data Loading Instructions

The user specifies in the application settings the path to a folder containing .xlsx files. All files in this folder are loaded into the DuckDB table (oz_video_products), and the data from each report is combined (unioned) together.

* **Target DuckDB Table:** oz_video_cover_products
* **File Pattern:** Pattern to identify files to process, e.g., "*.xlsx" in folder
* **Sheet Name:** "Озон.Видеообложка"
* **Header Row Number:** 2
* **Data Starts on Row:** 4
* **Columns to Load with Data Type Conversions/Transformations:** (List the exact Excel column header names and the corresponding target DuckDB table column names):

TABLE TITLE | TYPE | -- TITLE IN FILE
* oz_vendor_code VARCHAR,                 -- Артикул*
* video_cover_link TEXT,                  -- Озон.Видеообложка: ссылка
* **Pre-Update Action:** Delete all existing data before inserting new data
* **File Grouping:** Union all records from multiple files

## Пример содержимого листа "Озон.Видеообложка":
|  | Озон.Видеообложка |
|---|---|
| Артикул* | Озон.Видеообложка: ссылка |
| Обязательное поле |  |
| 1632000118-серый-40https://cdnvideo.v.ozone.ru/vod/video-52/01J6F3ASC0S146HY9CYC6K8FY8/asset_3_h264.mp4 | https://cdnvideo.v.ozone.ru/vod/video-52/01J6F3ASC0S146HY9CYC6K8FY8/asset_3_h264.mp4  |
| 1632000124-синий-38 | https://cdnvideo.v.ozone.ru/vod/video-53/01J87HXPHGQMWJX04C2092Y8FN/asset_2_h264.mp4  |
| 0948000050-серый-41 | https://cdnvideo.v.ozone.ru/vod/video-43/01HXGJYN2GB3J2Q74XJGBX3TCF/asset_2_h264.mp4 |