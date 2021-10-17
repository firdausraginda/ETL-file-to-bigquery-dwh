import pandas as pd
from bigquery.setup import create_bq_client
import pandas_gbq

def transform_precipitation():

    sql = """
        SELECT
            PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS date,
            SAFE_CAST(precipitation AS FLOAT64) AS precipitation,
            SAFE_CAST(precipitation_normal AS FLOAT64) AS precipitation_normal,
        FROM
        `dummy-329203.project_1_staging.precipitation_inch`
        """

    client = create_bq_client()

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)    

    return df


def transform_temperature():

    sql = """
        SELECT
            PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS date,
            SAFE_CAST(min AS FLOAT64) AS min,
            SAFE_CAST(max AS FLOAT64) AS max,
            SAFE_CAST(normal_min AS FLOAT64) AS normal_min,
            SAFE_CAST(normal_max AS FLOAT64) AS normal_max
        FROM
        `dummy-329203.project_1_staging.temperature_degreef`
        """

    client = create_bq_client()

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)

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

            SAFE_CAST(eo.attributes_outdoor_seating AS bool) AS attributes_outdoor_seating,
            SAFE_CAST(eo.attributes_good_for_kids AS bool) AS attributes_good_for_kids,
            SAFE_CAST(eo.attributes_restaurants_good_for_groups AS bool) AS attributes_restaurants_good_for_groups,
            SAFE_CAST(eo.attributes_has_tv AS bool) AS attributes_has_tv,
            SAFE_CAST(eo.attributes_restaurants_take_out AS bool) AS attributes_restaurants_take_out,
            SAFE_CAST(eo.attributes_business_accepts_credit_cards AS bool) AS attributes_business_accepts_credit_cards,
            SAFE_CAST(eo.attributes_restaurants_price_range2 AS int) AS attributes_restaurants_price_range2,
            SAFE_CAST(eo.attributes_bike_parking AS bool) AS attributes_bike_parking,
            SAFE_CAST(eo.attributes_restaurants_reservations AS bool) AS attributes_restaurants_reservations,
            SAFE_CAST(eo.attributes_business_accepts_bitcoin AS bool) AS attributes_business_accepts_bitcoin,
            eo.attributes_restaurants_attire,
            eo.attributes_business_parking,
            SAFE_CAST(eo.attributes_caters AS bool) AS attributes_caters,
            eo.attributes_noise_level,
            eo.attributes_wifi,
            eo.attributes_ambience,
            eo.attributes_good_for_meal,
            SAFE_CAST(eo.attributes_restaurants_delivery AS bool) AS attributes_restaurants_delivery,
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


def transform_yelp_user():
    sql = """
        SELECT
            user_id,
            name,
            SAFE_CAST(review_count AS INT64) AS review_count,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', yelping_since) AS yelping_since,
            SAFE_CAST(useful AS INT64) AS useful,
            SAFE_CAST(funny AS INT64) AS funny,
            SAFE_CAST(cool AS INT64) AS cool,
            elite,
            friends,
            SAFE_CAST(fans AS INT64) AS fans,
            SAFE_CAST(average_stars AS FLOAT64) AS average_stars,
            SAFE_CAST(compliment_hot AS INT64) AS compliment_hot,
            SAFE_CAST(compliment_more AS INT64) AS compliment_more,
            SAFE_CAST(compliment_profile AS INT64) AS compliment_profile,
            SAFE_CAST(compliment_cute AS INT64) AS compliment_cute,
            SAFE_CAST(compliment_list AS INT64) AS compliment_list,
            SAFE_CAST(compliment_note AS INT64) AS compliment_note,
            SAFE_CAST(compliment_plain AS INT64) AS compliment_plain,
            SAFE_CAST(compliment_cool AS INT64) AS complimentcool,
            SAFE_CAST(compliment_funny AS INT64) AS compliment_funny,
            SAFE_CAST(compliment_writer AS INT64) AS compliment_writer,
            SAFE_CAST(compliment_photos AS INT64) AS compliment_photos

        FROM
            `dummy-329203.project_1_staging.yelp_user`
        """
    
    client = create_bq_client()

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)

    return df


def transform_yelp_checkin():
    sql = """
        SELECT
            business_id,
            date
        FROM
            `dummy-329203.project_1_staging.yelp_checkin`
        """
    
    client = create_bq_client()

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)


def transform_yelp_tip():
    sql = """
        SELECT
            concat(user_id, '-', business_id) as id,
            user_id,
            business_id,
            text,
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', date) AS date,
            SAFE_CAST(compliment_count AS INT64) AS compliment_count
        FROM
            `dummy-329203.project_1_staging.yelp_tip`
        """
    
    client = create_bq_client()

    # read table as dataframe
    df = pandas_gbq.read_gbq(sql, project_id=client.project)

    return df


if __name__ == "__main__":
    
    precipitation_df = transform_precipitation()
    temperature_df = transform_temperature()
    yelp_business_df = transform_yelp_business()
    yelp_user_df = transform_yelp_user()
    yelp_checkin = transform_yelp_checkin()
    yelp_tip = transform_yelp_tip()

    print(yelp_tip)
    print(yelp_tip.info())