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

    st.title("Bienvenido al Dashboard de MotoGP!")
    with st.sidebar:
        logo_path = os.path.join(base_directory, 'imgs', 'motogp_logo.png')

        logo = Image.open(logo_path)
        st.image(logo, width=(200))
        st.header('Secciones')

        selected = option_menu(
            menu_title=None,
            options=["Section1", "Section2", "Section3", "Section4"],
            icons=["1-square", "2-square", "3-square","4-square"],
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

        
    def RenderSection1():
        S1_col1,S1_col2 = st.columns([0.2,0.2])
        placeholder = st.empty()
        placeholder2 = st.empty()

        with S1_col1:
            racing_class = st.radio("select racing class:", key="racing_class",options= ["motogp", "250cc_moto2", "125cc_moto3", "moto-e"],index=0)

            
        with S1_col2:
            season= st.selectbox(
                    'Temporada',
                    (2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))
                        
        
        with S1_col1:
            
            df = fetch_cummulative_sum_points(season, racing_class)
            
            fig = px.line(df, x="num_round", y="cummulative_sum", color='rider_name', symbol="rider_name")
            # fig.show()
            st.plotly_chart(fig)

        with S1_col2:
            df2 = fetch_cummulative_sum_points_teams(season, racing_class)
            fig2 = px.bar(df2, x="des_constructor", y="total_points", color='des_constructor',)
            # fig.show() 
            st.plotly_chart(fig2)

    def RenderSection2():    
        S2_col1,S2_col2 = st.columns([0.2,0.2])
        barchart1= st.empty()
        barchart2= st.empty()
        barchart3= st.empty()
        barchart4= st.empty()
        stats1 = st.empty()
        # with S2_col1:
            # racing_class = st.radio("select racing class:", key="racing_class",options= ["motogp", "250cc_moto2", "125cc_moto3", "moto-e"],index=0)
        with S2_col1:
            season= st.selectbox(
                    'Temporada',
                    (2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))
                        
        with barchart1:
            motogp= "'motogp'"
            

            
            df1 = fetch_season_bar_chart(season, motogp)
            

            fig1 = px.bar(df1, x="rider_name", y="total_points", color='rider_name')
                            

            # fig.show()
            
            st.plotly_chart(fig1)
           
        with barchart2:
            
            inter = "Intermediate"
            
            df2 = fetch_season_bar_chart(season, inter)
            
            fig2 = px.bar(df2, x="rider_name", y="total_points", color='rider_name')
            
            st.plotly_chart(fig2)
            
        with barchart3:
            
            lower = "Lower Class"
            

            
            
            df3 = fetch_season_bar_chart(season, lower)
            

            
            fig3 = px.bar(df3, x="rider_name", y="total_points", color='rider_name')
            

            
            st.plotly_chart(fig3)
            
        with barchart4:
        
            motoe = "'moto-e'"

            df4 = fetch_season_bar_chart(season, motoe)

            
            fig4 = px.bar(df4, x="rider_name", y="total_points", color='rider_name')

            
            st.plotly_chart(fig4)

        # with stats1:
            

    def RenderSection3():

        S2_col1,S2_col2 = st.columns([0.2,0.2])
        map_tracks = st.empty()
        map_riders = st.empty()
        with S2_col1:
            season= st.selectbox(
                    'Temporada',
                    (2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))
        with map_tracks:
            df= fetch_track_location(season)
            fig5 = px.scatter_mapbox(df, lon= df['latitude'], lat= df['longitude'], zoom=1, color = df['des_track'],
                                     height=900, width=900, title= f'Track locatioin map in {season}')

            fig5.update_layout(mapbox_style="open-street-map")
            # fig5.show()
            st.plotly_chart(fig5)
        
        with map_riders:
            df= fetch_rider_location(season)
            fig5 = px.scatter_mapbox(df, lon= df['latitude'], lat= df['longitude'], zoom=1, color = df['rider_full_name'],
                                     height=900, width=900, title= f'Rider Birth Location map in {season}')

            fig5.update_layout(mapbox_style="open-street-map")
            # fig5.show()
            st.plotly_chart(fig5)

    def RenderSection4():
        S1_col1,S1_col2 = st.columns([0.2,0.2])
        placeholder = st.empty()

        with S1_col1:
            racing_class = st.radio("select racing class:", key="racing_class",options= ["motogp", "250cc_moto2", "125cc_moto3", "moto-e"],index=0)
        with S1_col2:
            season= st.selectbox(
                    'Temporada',
                    (2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023))
                        
        
        with placeholder:
            df = fetch_cummulative_sum_points_teams(season, racing_class)



    if st.session_state.active_tab == 'Section1':
        RenderSection1()

    elif st.session_state.active_tab == 'Section2':
        RenderSection2()
        
    elif st.session_state.active_tab == 'Section3':
        RenderSection3()


with main:

    #este bloque de código esta diseñado para poblar las tablas de equipos solo una vez
    # if st.session_state.datos_en_tablas == False:
    #         DatosTablaEquiposCaducidad()
    #         DatosTablaEquiposBandasH() 
    #         st.session_state.datos_en_tablas = True
    
    # #carga css
    # setup()

    #se muestra la página principal
    show_main_page()
