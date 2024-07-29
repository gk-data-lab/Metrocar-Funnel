-- Building Metrocar user and rides level funnel
-- downloads funnel_step 0

WITH downloads AS ( 
      SELECT
        app_downloads.platform AS platform,
        signups.age_range AS age_range,
        DATE(app_downloads.download_ts) AS download_dt,
        COUNT(app_downloads.app_download_key) AS user_count
        FROM app_downloads
        LEFT JOIN signups
        ON app_downloads.app_download_key=signups.session_id
        GROUP BY download_dt,age_range,platform
   ),
-- funnel_step 1- signup
    signup AS (
      SELECT
        app_downloads.platform AS platform,
        signups.age_range AS age_range,
        DATE(app_downloads.download_ts) AS download_dt,
        COUNT(DISTINCT signups.user_id) AS user_count
        FROM signups
        LEFT JOIN app_downloads
        ON app_downloads.app_download_key=signups.session_id
        GROUP BY download_dt,age_range,platform
   ),
   -- funnel_step 2- ride_request 
    ride_request AS (
      SELECT
       app_downloads.platform AS platform,
       signups.age_range AS age_range,
       DATE(app_downloads.download_ts) AS download_dt, 
       COUNT(DISTINCT ride_requests.user_id) AS user_count,
       COUNT(DISTINCT ride_requests.ride_id) AS ride_count
       FROM ride_requests
        LEFT JOIN signups
         ON ride_requests.user_id = signups.user_id
        LEFT JOIN app_downloads
         ON app_downloads.app_download_key=signups.session_id
      GROUP BY download_dt,age_range,platform   
    ),
    -- funnel_step 3 - ride_accepted
    ride_accepted AS (
      SELECT
       app_downloads.platform AS platform,
       signups.age_range AS age_range,
       DATE(app_downloads.download_ts) AS download_dt, 
       COUNT(DISTINCT ride_requests.user_id) AS user_count,
       COUNT(DISTINCT ride_requests.ride_id) AS ride_count
       FROM ride_requests
        LEFT JOIN signups
         ON ride_requests.user_id = signups.user_id
        LEFT JOIN app_downloads
         ON app_downloads.app_download_key=signups.session_id
      WHERE ride_requests.accept_ts IS NOT NULL
      GROUP BY download_dt,age_range,platform   
    ),
    -- funnel_step 4 - ride_pickup
    ride_pickup AS (
      SELECT
       app_downloads.platform AS platform,
       signups.age_range AS age_range,
       DATE(app_downloads.download_ts) AS download_dt, 
       COUNT(DISTINCT ride_requests.user_id) AS user_count,
       COUNT(DISTINCT ride_requests.ride_id) AS ride_count
       FROM ride_requests
        LEFT JOIN signups
         ON ride_requests.user_id = signups.user_id
        LEFT JOIN app_downloads
         ON app_downloads.app_download_key=signups.session_id
      WHERE ride_requests.pickup_ts IS NOT NULL
      GROUP BY download_dt,age_range,platform   
    ),
     -- funnel_step 5 - ride_completed
    ride_completed AS (
      SELECT
       app_downloads.platform AS platform,
       signups.age_range AS age_range,
       DATE(app_downloads.download_ts) AS download_dt, 
       COUNT(DISTINCT ride_requests.user_id) AS user_count,
       COUNT(DISTINCT ride_requests.ride_id) AS ride_count
       FROM ride_requests
        LEFT JOIN signups
         ON ride_requests.user_id = signups.user_id
        LEFT JOIN app_downloads
         ON app_downloads.app_download_key=signups.session_id
      WHERE ride_requests.dropoff_ts IS NOT NULL
      GROUP BY download_dt,age_range,platform   
    ),
    -- funnel_step 6 - payment 
    payment AS (
      SELECT
       app_downloads.platform AS platform,
       signups.age_range AS age_range,
       DATE(app_downloads.download_ts) AS download_dt, 
       COUNT(DISTINCT ride_requests.user_id) AS user_count,
       COUNT(DISTINCT ride_requests.ride_id) AS ride_count
       FROM ride_requests
        LEFT JOIN signups
         ON ride_requests.user_id = signups.user_id
        LEFT JOIN app_downloads
         ON app_downloads.app_download_key=signups.session_id
        LEFT JOIN transactions
         ON transactions.ride_id=ride_requests.ride_id
      WHERE transactions.charge_status = 'Approved' 
      GROUP BY download_dt,age_range,platform   
    ),
        -- funnel_step 7 - review 
    review AS (
      SELECT
       app_downloads.platform AS platform,
       signups.age_range AS age_range,
       DATE(app_downloads.download_ts) AS download_dt, 
       COUNT(DISTINCT reviews.user_id) AS user_count,
       COUNT(DISTINCT reviews.ride_id) AS ride_count
       FROM ride_requests
        LEFT JOIN signups
         ON ride_requests.user_id = signups.user_id
        LEFT JOIN app_downloads
         ON app_downloads.app_download_key=signups.session_id
        LEFT JOIN transactions
         ON transactions.ride_id=ride_requests.ride_id
        LEFT JOIN reviews
         ON reviews.ride_id=ride_requests.ride_id
      WHERE reviews.review_id IS NOT NULL 
      GROUP BY download_dt,age_range,platform   
    )
    SELECT 
        0 AS funnel_step,
        'download' AS funnel_name,
        platform,
        age_range,
        download_dt,
        user_count,
        0 AS ride_count
         FROM downloads
         
    UNION
    
    SELECT 
        1 AS funnel_step,
        'signup' AS funnel_name,
        platform,
        age_range,
        download_dt,
        user_count,
        0 AS ride_count
         FROM signup
         
         UNION
         
    SELECT 
        2 AS funnel_step,
        'ride_request' AS funnel_name,
        platform,
        age_range,
        download_dt,
        user_count,
        ride_count
         FROM ride_request
         
               UNION
         
    SELECT 
        3 AS funnel_step,
        'ride_accepted' AS funnel_name,
        platform,
        age_range,
        download_dt,
        user_count,
        ride_count
         FROM ride_accepted
                     
                     UNION
         
    SELECT 
        4 AS funnel_step,
        'ride_pickup' AS funnel_name,
        platform,
        age_range,
        download_dt,
        user_count,
        ride_count
         FROM ride_pickup
         
          
                     UNION
     SELECT 
        5 AS funnel_step,
        'ride_completed' AS funnel_name,
        platform,
        age_range,
        download_dt,
        user_count,
        ride_count
         FROM ride_completed
         
          
                     UNION
         
    SELECT 
        6 AS funnel_step,
        'payment' AS funnel_name,
        platform,
        age_range,
        download_dt,
        user_count,
        ride_count
         FROM payment

                  UNION
         
    SELECT 
        7 AS funnel_step,
        'review' AS funnel_name,
        platform,
        age_range,
        download_dt,
        user_count,
        ride_count
         FROM review
         ORDER BY funnel_step,platform,age_range,download_dt;

-- Data related to price surging

SELECT
       app_downloads.platform AS platform,
       signups.age_range AS age_range,
       to_char(ride_requests.request_ts,'HH24:00') AS requested_hours,
       COUNT(DISTINCT ride_requests.user_id) AS user_count,
       COUNT(DISTINCT ride_requests.ride_id) AS ride_count,
       SUM(ROUND(transactions.purchase_amount_usd)) AS total_revenue
       FROM ride_requests
        LEFT JOIN signups
         ON ride_requests.user_id = signups.user_id
        LEFT JOIN app_downloads
         ON app_downloads.app_download_key=signups.session_id
        LEFT JOIN transactions
         ON transactions.ride_id=ride_requests.ride_id
        WHERE transactions.charge_status = 'Approved'
      GROUP BY requested_hours,age_range,platform