import pandas as pd
from bigquery.setup import create_bq_client
import pandas_gbq


def sql_read_query(table_name):
    """define sql query to read table with given table name"""

    sql = """
        SELECT *
        FROM `dummy-329203.project_1_staging.{}`
        limit 1000
        """.format(table_name)

    return sql


def transform_precipitation():

    # get sql query to read table
    client = create_bq_client()
    table_name = 'precipitation_inch'
    sql = sql_read_query(table_name)

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)

    # convert date from int to datetime
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

    # convert precipitation & precipitation_normal to float with error handling to NaN
    df['precipitation'] = pd.to_numeric(df['precipitation'], errors='coerce')
    df['precipitation_normal'] = pd.to_numeric(df['precipitation_normal'], errors='coerce')

    return df


def transform_temperature():

    # get sql query to read table
    client = create_bq_client()
    table_name = 'temperature_degreef'
    sql = sql_read_query(table_name)

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)

    # convert date from int to datetime
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

    # convert min, max, normal_min, & normal_max to float with error handling to NaN
    df['min'] = pd.to_numeric(df['min'], errors='coerce')
    df['max'] = pd.to_numeric(df['max'], errors='coerce')
    df['normal_min'] = pd.to_numeric(df['normal_min'], errors='coerce')
    df['normal_max'] = pd.to_numeric(df['normal_max'], errors='coerce')

    return df


def transform_yelp_business():

    sql = """
        WITH
            extract_obj as (
            SELECT
                business_id,
                JSON_VALUE(attributes, '$.OutdoorSeating') AS attributes_outdoor_seating,
                JSON_VALUE(attributes, '$.GoodForKids') AS attributes_good_for_kids,
                JSON_VALUE(attributes, '$.RestaurantsGoodForGroups') AS attributes_restaurants_good_for_groups,
                JSON_VALUE(attributes, '$.HasTV') AS attributes_has_tv,
                JSON_VALUE(attributes, '$.RestaurantsTakeOut') AS attributes_restaurants_take_out,
                JSON_VALUE(attributes, '$.BusinessAcceptsCreditCards') AS attributes_business_accepts_credit_cards,
                JSON_VALUE(attributes, '$.RestaurantsPriceRange2') AS attributes_restaurants_price_range2,
                JSON_VALUE(attributes, '$.BikeParking') AS attributes_bike_parking,
                JSON_VALUE(attributes, '$.RestaurantsReservations') AS attributes_restaurants_reservations,
                JSON_VALUE(attributes, '$.BusinessAcceptsBitcoin') AS attributes_business_accepts_bitcoin,
                JSON_VALUE(attributes, '$.RestaurantsAttire') AS attributes_restaurants_attire,
                JSON_VALUE(attributes, '$.BusinessParking') AS attributes_business_parking,
                JSON_VALUE(attributes, '$.Caters') AS attributes_caters,
                JSON_VALUE(attributes, '$.NoiseLevel') AS attributes_noise_level,
                JSON_VALUE(attributes, '$.WiFi') AS attributes_wifi,
                JSON_VALUE(attributes, '$.Ambience') AS attributes_ambience,
                JSON_VALUE(attributes, '$.GoodForMeal') AS attributes_good_for_meal,
                JSON_VALUE(attributes, '$.RestaurantsDelivery') AS attributes_restaurants_delivery,
                JSON_VALUE(attributes, '$.Alcohol') AS attributes_alcohol,

                JSON_VALUE(hours, '$.Monday') AS hours_monday,
                JSON_VALUE(hours, '$.Tuesday') AS hours_tuesday,
                JSON_VALUE(hours, '$.Wednesday') AS hours_wednesday,
                JSON_VALUE(hours, '$.Thursday') AS hours_thursday,
                JSON_VALUE(hours, '$.Friday') AS hours_friday,
                JSON_VALUE(hours, '$.Saturday') AS hours_saturday,
                JSON_VALUE(hours, '$.Sunday') AS hours_sunday
            FROM
                `dummy-329203.project_1_staging.yelp_business`
            )

        SELECT
            business_id,
            name,
            address,
            city,
            state,
            postal_code,
            latitude,
            longitude,
            stars,
            review_count,
            is_open,
            categories,

            SAFE_CAST(eo.attributes_outdoor_seating AS bool) as attributes_outdoor_seating,
            SAFE_CAST(eo.attributes_good_for_kids AS bool) as attributes_good_for_kids,
            SAFE_CAST(eo.attributes_restaurants_good_for_groups AS bool) as attributes_restaurants_good_for_groups,
            SAFE_CAST(eo.attributes_has_tv AS bool) as attributes_has_tv,
            SAFE_CAST(eo.attributes_restaurants_take_out AS bool) as attributes_restaurants_take_out,
            SAFE_CAST(eo.attributes_business_accepts_credit_cards AS bool) as attributes_business_accepts_credit_cards,
            SAFE_CAST(eo.attributes_restaurants_price_range2 AS int) as attributes_restaurants_price_range2,
            SAFE_CAST(eo.attributes_bike_parking AS bool) as attributes_bike_parking,
            SAFE_CAST(eo.attributes_restaurants_reservations AS bool) as attributes_restaurants_reservations,
            SAFE_CAST(eo.attributes_business_accepts_bitcoin AS bool) as attributes_business_accepts_bitcoin,
            eo.attributes_restaurants_attire,
            eo.attributes_business_parking,
            SAFE_CAST(eo.attributes_caters AS bool) as attributes_caters,
            eo.attributes_noise_level,
            eo.attributes_wifi,
            eo.attributes_ambience,
            eo.attributes_good_for_meal,
            SAFE_CAST(eo.attributes_restaurants_delivery AS bool) as attributes_restaurants_delivery,
            eo.attributes_alcohol,

            eo.hours_monday,
            eo.hours_tuesday,
            eo.hours_wednesday,
            eo.hours_thursday,
            eo.hours_friday,
            eo.hours_saturday,
            eo.hours_sunday

        FROM `dummy-329203.project_1_staging.yelp_business` yb
        LEFT JOIN extract_obj eo USING(business_id)
        """
    client = create_bq_client()

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)    

    return df


if __name__ == "__main__":
    
    # precipitation_df = transform_precipitation()
    # temperature_df = transform_temperature()
    yelp_business_df = transform_yelp_business()

    print(yelp_business_df)
    print(yelp_business_df.info())