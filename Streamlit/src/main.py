import pandas as pandas
import plotly.express as px
import streamlit as st
from PIL import Image
import os
from streamlit_option_menu import option_menu
from postgre_connection import * 


base_directory = os.path.dirname(os.path.realpath(os.path.dirname(__file__)))
image_path = os.path.join(base_directory, 'imgs', 'motogp_logo.png')

st.set_page_config(page_title="MotoGP Dashboard", layout="wide", page_icon=Image.open(image_path)) 

main = st.container()
col1, col2 = st.columns([.01,9.99])
headerSection = st.container()
Section1 = st.container()
Section2 = st.container()
Section3 = st.container()
regSection1, regSection2, regSection3, regSection4= st.columns(4)
# loginSection1, loginSection2, loginSection3, loginSection4= st.columns(4)
menuSection1, menuSection2, menuSection3, menuSection4,  menuSection5 = st.columns(5)
vv1, vv2, vv3 = st.columns(3)
header1 = st.empty()
header2 = st.container()
i=1
logOutSection = st.container()
registering = False


def show_main_page():
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = 'Section1'

    st.title("Welcome to the MotoGP Dashboard!")
    with st.sidebar:
        logo_path = os.path.join(base_directory, 'imgs', 'motogp_logo.png')

        logo = Image.open(logo_path)
        st.image(logo, width=(200))
        st.header('Secciones')

        selected = option_menu(
            menu_title=None,
            options=["Section1", "Section2", "Section3", "Section4", "Section5"],
            icons=["1-square", "2-square", "3-square","4-square", "5-square"],
            styles={
                "container": {"padding": ".5rem !important", "background-color": "#fff"},
                "icon": {"color": "inherit"},
                "nav-link": {
                    "font-family": 'Raleway, sans serif !important;',
                    "text-align": "left",
                    "margin": "3px",
                    "--hover-color": "#FEDCF3",

                },
                "nav-link-selected": {
                    "background-color": "#E4129F",
                    "font-weight":"600"
                    },
            },
        )

    if selected == "Section1":
        st.session_state.active_tab = 'Section1'

    elif selected == "Section2":
        st.session_state.active_tab = 'Section2'

    elif selected == "Section3":
        st.session_state.active_tab = 'Section3'

    elif selected == "Section4":
        st.session_state.active_tab = 'Section4'
    elif selected == "Section5":
        st.session_state.active_tab = 'Section5'

        
    def RenderSection1():
        S1_col1,S1_col2,S1_col3 = st.columns([0.2,0.2,0.2])
        placeholder = st.empty()
        placeholder2 = st.empty()
        S1_tab1, S1_tab2, S1_tab3 = st.tabs(["Riders", "Constructors", "Teams"])

        with S1_col1:
            racing_class = st.radio("select racing class:", key="racing_class",options= ["motogp", "250cc_moto2", "125cc_moto3", "moto-e"],index=0)

            
        with S1_col2:
            season= st.selectbox(
                    'Temporada',
                    (2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))
                        
        
        with S1_tab1:
            
            df = fetch_cummulative_sum_points(season, racing_class)
            
            fig = px.line(df, x="num_round", y="cummulative_sum", color='rider_full_name', symbol="rider_full_name")
            # fig.show()
            st.plotly_chart(fig)

        with S1_tab2:
            df2 = fetch_cummulative_sum_points_constructors(season, racing_class)
            fig2 = px.bar(df2, x="des_constructor", y="total_points", color='des_constructor',)
            # fig.show() 
            st.plotly_chart(fig2)

        with S1_tab3:
            
            df3 = fetch_cummulative_sum_points_teams(season, racing_class)
            fig3 = px.bar(df3, x="des_team", y="total_points", color='des_team',)
            # fig.show() 
            st.plotly_chart(fig3)


    def RenderSection2():    
        S2_col1,S2_col2 = st.columns([0.2,0.2])
        barchart1= st.empty()
        barchart2= st.empty()
        barchart3= st.empty()
        barchart4= st.empty()
        stats1 = st.empty()
        S2_tab1, S2_tab2, S2_tab3,S2_tab4 = st.tabs(["MotoGP", "Moto2/250cc", "Moto3/125cc", "MotoE"])

        # with S2_col1:
            # racing_class = st.radio("select racing class:", key="racing_class",options= ["motogp", "250cc_moto2", "125cc_moto3", "moto-e"],index=0)
        with S2_col1:
            season= st.selectbox(
                    'Temporada',
                    (2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))

        with S2_tab1:                
            # with barchart1:
            motogp= "'motogp'"
            st.write(f'{season} MotoGP rider points and classification')
            df1 = fetch_season_bar_chart(season, motogp)
            

            fig1 = px.bar(df1, x='rider_name', y="total_points", color='rider_name')
                                

            # fig.show()
            
            st.plotly_chart(fig1)
            # with barchart3:
        with S2_tab2:  
                
            inter = "Intermediate"
            st.write(f'{season} Moto2/250cc rider points and classification')
            df2 = fetch_season_bar_chart(season, inter)
            
            fig2 = px.bar(df2, x="rider_name", y="total_points", color='rider_name')
            
            st.plotly_chart(fig2)

        with S2_tab3:
            lower = "Lower Class"
            
            st.write(f'{season} Moto3/125cc rider points and classification')
            
            
            df3 = fetch_season_bar_chart(season, lower)
            

            
            fig3 = px.bar(df3, x="rider_name", y="total_points", color='rider_name')
            

            
            st.plotly_chart(fig3)
                
        with S2_tab4:
            
            motoe = "'moto-e'"
            st.write(f'{season} MotoE rider points and classification')
            df4 = fetch_season_bar_chart(season, motoe)

            
            fig4 = px.bar(df4, x="rider_name", y="total_points", color='rider_name')

            
            st.plotly_chart(fig4)

            
            

    def RenderSection3():

        S3_col1,S3_col2 = st.columns([0.2,0.2])
        map_tracks = st.empty()
        map_riders = st.empty()
        S3_tab1, S3_tab2 = st.tabs(["Grand Prix", "Riders"])
        
        with S3_col1:    
            season= st.selectbox(
                    'Temporada',
                    (2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))
        with S3_tab1:
            df= fetch_track_location(season)
            fig5 = px.scatter_mapbox(df, lon= df['latitude'], lat= df['longitude'], zoom=1, color = df['des_track'],
                                     height=900, width=900, title= f'Track locatioin map in {season}')

            fig5.update_layout(mapbox_style="open-street-map")
            st.plotly_chart(fig5)
        
        # with map_riders:
        with S3_tab2:
            
            df= fetch_rider_location(season)
            fig5 = px.scatter_mapbox(df, lon= df['latitude'], lat= df['longitude'], zoom=1, color = df['rider_full_name'],
                                     height=900, width=900, title= f'Rider Birth Location map in {season}')

            fig5.update_layout(mapbox_style="open-street-map")
            # fig5.show()
            st.plotly_chart(fig5)

    def RenderSection4():
        S4_col1,S4_col2 = st.columns([0.2,0.2])
        S42_col1,S42_col2,S42_col3,S42_col4 = st.columns([0.2,0.2,0.2,0.2])
        
        S2_tab1, S2_tab2, S2_tab3 = st.tabs(["Riders", "Constructors","Consecutives"])

        empty = st.empty()

        with S4_col1:
            st.write("Season/Class specific statistics")
            racing_class = st.radio("select racing class:", key="racing_class",options= ["Any","motogp", "250cc_moto2", "125cc_moto3", "moto-e"],index=0)
        with S4_col2:
            # season= st.selectbox(
            #         'Temporada',
            #         ('Any',2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))
            season2=st.slider('Select season', 2002, 2023,(2002, 2023))            
        
        
        with S42_col1:
            num_gp = fetch_total_num_gp(season2)
            if st.session_state.UsingCSV:
                st.metric(label='number of GPs', value=num_gp.iloc[0])
            else:
                st.metric(label='number of GPs', value=num_gp[0])
        with S42_col2:
            num_hp = fetch_HP_races(season2,racing_class)
            if st.session_state.UsingCSV:
                st.metric(label='number of half point races', value=num_hp.iloc[0])
            else:
                st.metric(label='number of half point races', value=num_hp[0])
        with S42_col3:
            num_night=fetch_night_races(season2)
            if st.session_state.UsingCSV:
                st.metric(label='number of night races', value=num_night.iloc[0])
            else:
                st.metric(label='number of night races', value=num_night[0])
        with S42_col4:
            num_sat=fetch_satruday_races(season2)
            if st.session_state.UsingCSV:
                st.metric(label='number of saturday races', value=num_sat.iloc[0])
            else:
                st.metric(label='number of saturday races', value=num_sat[0])
            
        with S2_tab1:
            S43_col1,S43_col2 = st.columns([0.2,0.2])
            with S43_col1:
                top_wins = fetch_top_wins(season2,racing_class)
                st.dataframe(top_wins)

                top_wins_sprint = fetch_top_wins_sprint(season2,racing_class)
                st.dataframe(top_wins_sprint)

                top_poles = fetch_top_poles(season2,racing_class)
                st.dataframe(top_poles)
                
                top_fast_laps = fetch_top_percentage_points(season2,racing_class)
                st.dataframe(top_fast_laps)

                top_percentage_wins_season =fetch_top_percentage_wins_season(season2, racing_class)
                st.dataframe(top_percentage_wins_season)

                top_different_podium_finishers =fetch_top_different_podium_finishers(season2, racing_class)
                st.dataframe(top_different_podium_finishers)

                top_percentage_points_carreer =fetch_top_percentage_points_carreer(season2, racing_class)
                st.dataframe(top_percentage_points_carreer)

            with S43_col2:
                top_podiums = fetch_top_podiums(season2,racing_class)
                st.dataframe(top_podiums)
                st.divider()

                top_podiums_sprint = fetch_top_podiums_sprint(season2,racing_class)
                st.dataframe(top_podiums_sprint)

                top_fast_laps = fetch_top_fast_laps(season2,racing_class)
                st.dataframe(top_fast_laps)

                top_points_career =fetch_top_points_carrer(season2, racing_class)
                st.dataframe(top_points_career)

                top_different_winners =fetch_top_different_winners(season2, racing_class)
                st.dataframe(top_different_winners)

                top_wins_by_track =fetch_top_wins_by_track(season2, racing_class)
                st.dataframe(top_wins_by_track)
              

        with S2_tab2:   
            S43_col1,S43_col2 = st.columns([0.2,0.2])
            with S43_col1:
                top_points_constructor =fetch_top_points_constructor(season2, racing_class)
                st.dataframe(top_points_constructor)


                top_wins_constructor =fetch_top_wins_constructor(season2, racing_class)
                st.dataframe(top_wins_constructor)

                
                top_podiums_constructor =fetch_top_podiums_constructor(season2, racing_class)
                st.dataframe(top_podiums_constructor)

                top_percentage_wins_season_constructor =fetch_top_percentage_wins_season_constructor(season2, racing_class)
                st.dataframe(top_percentage_wins_season_constructor)

            with S43_col2:
                top_percentage_podiums_season_constructor =fetch_top_percentage_podiums_season_constructor(season2, racing_class)
                st.dataframe(top_percentage_podiums_season_constructor)

                top_percentage_podiums_season_teams =fetch_top_percentage_podiums_season_teams(season2, racing_class)
                st.dataframe(top_percentage_podiums_season_teams)

                top_wins_by_track_constructor =fetch_top_wins_by_track_constructor(season2, racing_class)
                st.dataframe(top_wins_by_track_constructor)




    def RenderSection5():
        S4_col1,S4_col2 = st.columns([0.2,0.2])
        S42_col1,S42_col2,S42_col3,S42_col4 = st.columns([0.2,0.2,0.2,0.2])
        
        S2_tab1, S2_tab2 = st.tabs(["Streaks", "Longest/shortest time to"])

        S43_col1,S43_col2 = st.columns([0.2,0.2])

        with S4_col1:
            st.write("Season/Class specific statistics")
            racing_class = st.radio("select racing class:", key="racing_class",options= ["Any","motogp", "250cc_moto2", "125cc_moto3"],index=0)
        with S4_col2:
            # season= st.selectbox(
            #         'Temporada',
            #         ('Any',2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))
            season2=st.slider('Select season', 2002, 2023,(2002, 2023))         
        placeholderX = st.container()
        placeholderY = st.container()

        with S2_tab1:

            st.write("Top 10 Finish Streaks:")   

            consecutive_results= most_consecutive_finishes(season2, racing_class)
            st.dataframe(consecutive_results)

            st.write("Top 10 Podium Streaks:")   

            consecutive_podiums= most_consecutive_podiums(season2, racing_class)
            st.dataframe(consecutive_podiums)
            
            st.write("Top 10 Wins Streaks:")   

            consecutive_wins= most_consecutive_wins(season2, racing_class)
            st.dataframe(consecutive_wins)
            
            st.write("Top 10 Fail Streaks:")   

            consecutive_fails= most_consecutive_fails(season2, racing_class)
            st.dataframe(consecutive_fails)

            st.write("oldest winner")
            oldest_winners = oldest_winner(season2, racing_class)
            st.dataframe(oldest_winners)
            
            

        with S2_tab2:
            
            st.write("Least time to first win")
            wins = soonest_win(season2, racing_class)
            st.dataframe(wins)

            st.write("longest time to first win")
            longest_wins = longest_win(season2, racing_class)
            st.dataframe(longest_wins)

            st.write("longest career without a win")
            winless_streak = longest_winless_streak(season2, racing_class)
            st.dataframe(winless_streak)

            st.write("youngest winner")
            youngest_winners = youngest_winner(season2, racing_class)
            st.dataframe(youngest_winners)

            
                
            
            
    if st.session_state.active_tab == 'Section1':
        RenderSection1()

    elif st.session_state.active_tab == 'Section2':
        RenderSection2()
        
    elif st.session_state.active_tab == 'Section3':
        RenderSection3()
    elif st.session_state.active_tab == 'Section4':
        RenderSection4()
    elif st.session_state.active_tab == 'Section5':
        RenderSection5()


with main:

    #este bloque de código esta diseñado para poblar las tablas de equipos solo una vez
    # if st.session_state.datos_en_tablas == False:
    #         DatosTablaEquiposCaducidad()
    #         DatosTablaEquiposBandasH() 
    #         st.session_state.datos_en_tablas = True
    
    # #carga css
    # setup()

    #se muestra la página principal

    if 'UsingCSV' not in st.session_state:
        if st.secrets['UsingCSV']=='True':
            st.session_state.UsingCSV =  True
        else:
            st.session_state.UsingCSV =  False


    if st.session_state.UsingCSV:
        connect_csv()
    show_main_page()
