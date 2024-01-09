import requests

def get_movies_from_api():
    url = "https://kinopoiskapiunofficial.tech/api/v2.2/films?order=RATING&type=ALL&ratingFrom=0&ratingTo=10&yearFrom=2023&yearTo=3000"
    headers = {
        "X-API-KEY": "your_api_key"
    }
    response = requests.get(url, headers=headers)
    pages = response.json().get("totalPages")
    movies = []
    for page in range(1, pages+1):
        url_page = f"https://kinopoiskapiunofficial.tech/api/v2.2/films?order=RATING&type=ALL&ratingFrom=0&ratingTo=10&yearFrom=2023&yearTo=3000&page={page}"
        response_page = requests.get(url_page, headers=headers)
        movies.extend(response_page.json().get("items"))

    return movies


from neo4j import GraphDatabase


def create_movie_node(movie):
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "mynewpassword"

    driver = GraphDatabase.driver(uri, auth=(user, password))
    session = driver.session()

    query = (
        "MERGE (m:Movie {Id: $kinopoiskId}) "
        "ON CREATE SET "
        "m.title = $nameRu, "
        "m.rating = $ratingKinopoisk, "
        "m.year = $year, "
        "m.type = $type, "
        "m.countries = $countries, "
        "m.genres = $genres"
    )

    session.run(
        query,
        kinopoiskId=movie.get("kinopoiskId"),
        nameRu=movie.get("nameRu") if movie.get("nameRu") is not None else movie.get("nameEn"),
        ratingKinopoisk=movie.get("ratingKinopoisk"),
        year=movie.get("year"),
        type=movie.get("type"),
        countries=[country.get("country") for country in movie.get("countries")],
        genres=[genre.get("genre") for genre in movie.get("genres")]
    )

    session.close()
    driver.close()


def get_persons_from_api(kinopoisk_id):
    url = f"https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={kinopoisk_id}"
    headers = {
        "X-API-KEY": "your_api_key"
    }
    response = requests.get(url, headers=headers)
    persons = response.json()

    return persons

def create_person_nodes(kinopoisk_id, persons):
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "mynewpassword"

    driver = GraphDatabase.driver(uri, auth=(user, password))
    session = driver.session()

    for person in persons:
        query = (
            "MERGE (p:Person {staffId: $staffId}) "
            "ON CREATE SET "
            "p.nameRu = $nameRu, "
            "p.professionKey = $professionKey"
        )

        session.run(
            query,
            staffId=person.get("staffId"),
            nameRu=person.get("nameRu"),
            professionKey=person.get("professionKey")
        )
        if person.get("professionKey") == "DIRECTOR":
            query_dir = (
                "MATCH (m:Movie {Id: $kinopoiskId}), (p:Person {staffId: $staffId}) "
                "MERGE (p)-[r:DIRECTED]->(m)"
            )
            session.run(query_dir, kinopoiskId=kinopoisk_id, staffId=person.get("staffId"))
        elif person.get("professionKey") == "ACTOR":
            query_act = (
                "MATCH (m:Movie {Id: $kinopoiskId}), (p:Person {staffId: $staffId}) "
                "MERGE (p)-[r:ACTED_IN]->(m)"
            )
            session.run(query_act, kinopoiskId=kinopoisk_id, staffId=person.get("staffId"))
        elif person.get("professionKey") == "PRODUCER":
            query_prod = (
                "MATCH (m:Movie {Id: $kinopoiskId}), (p:Person {staffId: $staffId}) "
                "MERGE (p)-[r:PRODUCED]->(m)"
            )
            session.run(query_prod, kinopoiskId=kinopoisk_id, staffId=person.get("staffId"))
        elif person.get("professionKey") == "WRITER":
            query_wrt = (
                "MATCH (m:Movie {Id: $kinopoiskId}), (p:Person {staffId: $staffId}) "
                "MERGE (p)-[r:WROTE]->(m)"
            )

            session.run(query_wrt, kinopoiskId=kinopoisk_id, staffId=person.get("staffId"))

    session.close()
    driver.close()

movies = get_movies_from_api()

for movie in movies:
    create_movie_node(movie)
    kinopoisk_id = movie.get("kinopoiskId")
    persons = get_persons_from_api(kinopoisk_id)
    create_person_nodes(kinopoisk_id, persons)