from neo4j import GraphDatabase, basic_auth


# connection with authentication
driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"), encrypted=False)

# connection without authentication
# driver = GraphDatabase.driver("bolt://localhost", encrypted=False)

session = driver.session()
transaction = session.begin_transaction()

f = open("output.txt",'w', encoding="utf-8")

# [Q1] List the first 20 actors in descending order of the number of films they acted in.
result = transaction.run("MATCH (n:Person) -[:ACTS_IN]-> (m:Movie)  RETURN n.name, count(m.title) ORDER BY count(m.title) DESC LIMIT 20")
f.write('### Q1 ###')
f.write('\n')
for record in result:
   f.write(record['n.name'] + ", " + str(record['count(m.title)']))
   f.write('\n')
f.write('\n')

# [Q2] List the titles of all movies with a review with at most 3 stars. OUTPUT: movie title
result = transaction.run("MATCH (u:User) -[r:RATED]-> (m:Movie) WHERE r.stars<=3 RETURN m.title ")
f.write('### Q2 ###')
f.write('\n')
for record in result:
    f.write(record['m.title'])
    f.write('\n')
f.write('\n')

# [Q3] Find the movie with the largest cast, out of the list of movies that have a review. OUTPUT: movie_title, number_of_cast_members
result = transaction.run("MATCH (u:User) -[:RATED]-> (m:Movie) <-[:ACTS_IN]- (n:Person) RETURN count(n.id), m.title ORDER BY count(n.id) DESC")
f.write('### Q3 ###')
f.write('\n')
for record in result:
    f.write(record['m.title'] + ", " + str(record['count(n.id)']))
    f.write('\n')
f.write('\n')

# [Q4] Find all the actors who have worked with at least 3 different directors (regardless of how many movies they acted in). For example, 3 movies with one director each would satisfy this (provided the directors where different), but also a single movie with 3 directors would satisfy it as well. OUTPUT: actor_name, number_of_directors_he/she_has_worked_with
result = transaction.run("MATCH (n:Person) -[:ACTS_IN]-> (m:Movie) <-[:DIRECTED]- (d:Director) WITH n.name as name, collect(d.id) AS dc WHERE length(dc)>=3 RETURN name, length(dc)")
f.write('### Q4 ###')
f.write('\n')
for record in result:
    f.write(record['name'] + ", " + str(record['length(dc)']))
    f.write('\n')
f.write('\n')

# [Q5] The Bacon number of an actor is the length of the shortest path between the actor and Kevin Bacon in the "co-acting" graph. That is, Kevin Bacon has Bacon number 0; all actors who acted in the same movie as him have Bacon number 1; all actors who acted in the same film as some actor with Bacon number 1 have Bacon number 2, etc. List all actors whose Bacon number is exactly 2 (first name, last name). You can familiarize yourself with the concept, by visiting The Oracle of Bacon. OUTPUT: actor_name
result = transaction.run("MATCH (kevin:Person {name: 'Kevin Bacon'}) -[:ACTS_IN]-> (m:Movie) <-[:ACTS_IN]- (coactor:Person) MATCH (coactor) -[:ACTS_IN]-> (m:Movie) <-[:ACTS_IN]- (cocoactor:Person) WHERE NOT cocoactor.name='Kevin Bacon' RETURN cocoactor.name")
f.write('### Q5 ###')
f.write('\n')
for record in result:
    f.write(record['cocoactor.name'] )
    f.write('\n')
f.write('\n')

# [Q6] List which genres have movies where Tom Hanks starred in.
result = transaction.run("MATCH (n:Person {name: 'Tom Hanks'}) -[:ACTS_IN]-> (m:Movie)  RETURN DISTINCT m.genre as mg")
f.write('### Q6 ###')
f.write('\n')
for record in result:
    f.write(record['mg'])
    f.write('\n')
f.write('\n')

# [Q7] Show which directors have directed movies in at least 2 different genres.
result = transaction.run("MATCH (d:Director) -[:DIRECTED]-> (m:Movie) WITH d.name as name, collect(m.genre) AS ge WHERE length(ge) >= 2  RETURN name, length(ge)")
f.write('### Q7 ###')
f.write('\n')
for record in result:
    f.write(record['name'] + ", " + str(record['length(ge)']))
    f.write('\n')
f.write('\n')

# [Q8] Show the top 5 pairs of actor, director combinations, in descending order of frequency of occurrence.
result = transaction.run("MATCH (n:Person) -[:ACTS_IN]-> (m:Movie) <-[:DIRECTED]- (d:Director) WITH d.name as name, n.name as nname, collect(n.id) AS dc  RETURN DISTINCT name, nname, length(dc) ORDER BY length(dc) DESC LIMIT 5  ")
f.write('### Q8 ###')
f.write('\n')
for record in result:
    f.write(record['name'] + ", " + record['nname'] +  ", " + str(record['length(dc)']))
    f.write('\n')
f.write('\n')





transaction.close()
session.close()
