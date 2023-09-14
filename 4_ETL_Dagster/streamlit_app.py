import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import altair as alt

# Connect Database and fetch data
@st.cache_data
def fetch_data():

    load_dotenv()

    conn = None

    # Load data
    try:
        # Set up connect to database
        conn = psycopg2.connect(database=os.getenv('DB_NAME'),
                                user=os.getenv('DB_USER'),
                                password=os.getenv('DB_PASS'),
                                host=os.getenv('DB_HOST'),
                                port=os.getenv('DB_PORT'))
        
        print("Database connected successfully")

        # Create a cursor
        cur = conn.cursor()

        # Select data from 'greentaxi' table
        cur.execute("""SELECT passenger_count,
                            trip_distance,
                            rate_code_des,
                            pmt_type_des,
                            pu_hour,
                            do_hour,
                            travel_day,
                            congestion_surcharge,
                            total_amount,
                            fee_per_mile
                        FROM greentaxi
                        WHERE total_amount > 0 And trip_distance > 0
                    """)

        rows = cur.fetchall()

        data_rm_na = pd.DataFrame(rows, columns = ["passenger_count", "trip_distance", "rate_code_des",
                                                    "pmt_type_des", "pu_hour", "do_hour", 
                                                    "travel_day", "congestion_surcharge", "total_amount", "fee_per_mile"])
        
        cur.close()

    except Exception as error:

        print(error)

    finally:    
        # Close communication with the database
        if conn is not None:
            conn.close()
    
    return data_rm_na

# Create Streamlit app
st.set_page_config(layout="wide", page_title="NYC Green Taxi Dashboard", initial_sidebar_state = "expanded", page_icon=":taxi:")

data_rm_na = fetch_data()

with st.container():
    st.write("""
            ### NYC Green Taxi Dashboard
            
            ##### Dashboard of green taxi trips in northern Manhattan and in the outer boroughs 
            """)
    st.write("---")

st.sidebar.header("User Input Selection")

with st.sidebar:

    rate_code = st.selectbox(
        "Select the rate code",
        ("All", "Standard rate", "JFK", "Newark", "Nassau or Westchester", "Negotiated fare", "Group ride", "Unknown")
    )

    payment_type = st.selectbox(
        "Select the payment type",
        ["All", "Credit card", "Cash", "No charge", "Dispute", "Unknown", "Voided trip"]
    )

    travel_day = st.selectbox(
        "Select the travel day",
        ["All", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    )


if rate_code != "All" and payment_type != "All" and travel_day != "All":
    select_data = data_rm_na[(data_rm_na["rate_code_des"].isin([rate_code])) & (data_rm_na["pmt_type_des"].isin([payment_type])) & (data_rm_na["travel_day"].isin([travel_day]))]
elif rate_code == "All" and payment_type != "All" and travel_day != "All":
    select_data = data_rm_na[(data_rm_na["pmt_type_des"].isin([payment_type])) & (data_rm_na["travel_day"].isin([travel_day]))]
elif rate_code != "All" and payment_type == "All" and travel_day != "All":
    select_data = data_rm_na[(data_rm_na["rate_code_des"].isin([rate_code])) & (data_rm_na["travel_day"].isin([travel_day]))]
elif rate_code != "All" and payment_type != "All" and travel_day == "All":
    select_data = data_rm_na[(data_rm_na["rate_code_des"].isin([rate_code])) & (data_rm_na["pmt_type_des"].isin([payment_type]))]
elif rate_code != "All" and payment_type == "All" and travel_day == "All":
    select_data = data_rm_na[(data_rm_na["rate_code_des"].isin([rate_code]))]
elif rate_code == "All" and payment_type != "All" and travel_day == "All":
    select_data = data_rm_na[(data_rm_na["pmt_type_des"].isin([payment_type]))]
elif rate_code == "All" and payment_type == "All" and travel_day != "All":
    select_data = data_rm_na[(data_rm_na["travel_day"].isin([travel_day]))]
else:
    select_data = data_rm_na

with st.container():
    col1, col2 = st.columns(2)
    with col1:
        st.write("""
                 ###### Total Amount and Trip Counts By Hour
                 """)
        data_chart1 = select_data[['total_amount', "pu_hour"]].groupby("pu_hour").agg("sum")
        data_chart1.reset_index(inplace=True)

        bar_chart = alt.Chart(data_chart1).mark_bar(color = "#78c679").encode(x=alt.X("pu_hour:O").title("pickup hour"),
                                                                              y=alt.Y("total_amount:Q").title("total amount, $"))

        col1.altair_chart(bar_chart, use_container_width=True)

    with col2:
        st.write("""
                ###### Distribution of Trips by Day per Hour
                """)

        data_chart2 = select_data[["total_amount", "travel_day", "pu_hour"]].groupby(["travel_day", "pu_hour"]).agg("count")
        data_chart2.reset_index(inplace=True)

        mark_bar = alt.Chart(data_chart2).mark_bar(opacity=0.7).encode(x=alt.X("pu_hour:O").title("pickup hour"), 
                                                                        y=alt.Y("total_amount:Q", stack = "normalize").title("count of trips"),
                                                                        color = alt.Color("travel_day:O").scale(scheme="yellowgreenblue"))
        
        col2.altair_chart(mark_bar, use_container_width=True)
    
    col3, col4 = st.columns(2)
    with col3:
        st.write("""
                 ###### Trip Distance and Total Amount   
                 """)
        data_chart3 = select_data[["total_amount", "trip_distance", "passenger_count"]]

        # Remove outliers
        data_chart3 = data_chart3[data_chart3["trip_distance"] < 1000]

        scatter_chart = alt.Chart(data_chart3).mark_circle().encode(x=alt.X("trip_distance:Q").title("trip distance, miles"), 
                                                                    y=alt.Y("total_amount:Q").title("total amount, $"), 
                                                                    size="passenger_count", 
                                                                    color=alt.Color("passenger_count:Q").scale(scheme="yellowgreenblue"), 
                                                                    tooltip=["trip_distance", "total_amount", "passenger_count"]) 
    
        col3.altair_chart(scatter_chart, use_container_width=True)

    with col4:
        st.write("""
                ###### Boxplot of Fee per Mile by Hour
                """)
        data_chart = select_data[["trip_distance", "fee_per_mile", "passenger_count", "pu_hour"]]
        
        # Remove outliers
        data_chart = data_chart[data_chart["trip_distance"] < 1000]
        data_chart = data_chart[data_chart["fee_per_mile"] < 50]

        mark_boxplot = alt.Chart(data_chart).mark_boxplot(ticks=True, extent="min-max", color="yellowgreen").encode(x=alt.X("pu_hour:O").title("pickup hour"), 
                                                                                                                    y=alt.Y("fee_per_mile:Q", 
                                                                                                                    scale=alt.Scale(zero=False)))

        col4.altair_chart(mark_boxplot, use_container_width=True)