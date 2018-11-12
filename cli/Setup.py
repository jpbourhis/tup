import os
import nltk
from Config import Config
from database import Database, SchemaGraph
from nlp import DBCorpusGenerator, DBCorpusClassifier, SQLGrammarClassifier
from _mysql_exceptions import OperationalError

class Setup(object):
    """
    Setup required items for NL2SQL
    """
    def __init__(self, communicator):
        self.config = Config()
        self.comm = communicator

    def test(self, force):
        print "Test Mod ON"
        try:
            database = Database()
        except ValueError as exception:
            self.comm.error("Error: %s" % exception)

        base_path = self.config.get('PATHS', 'base')
        graph_path = os.path.join(base_path, "graph.p")

        base_path = self.config.get('PATHS', 'base')
        corpus_path = os.path.join(base_path, "database_corpus.p")
        gen = DBCorpusGenerator(self.config.get('PATHS', 'stanford_jar'))

        "New Version"
        # go thourgh the database and save the schema
        #SchemaGraph().construct_without_id(database, corpus_path,self.config.get('PATHS', 'stanford_jar'))
        gen.construct_without_id(database, corpus_path)


    def run(self, force):
        self.comm.say("Setting up NL2SQL.")

        # Download wordnet corpora
        self.comm.say("Downloading WordNet corpora.")
        nltk.download("wordnet")

        # Menu to Save Creditential and Populate DB and Graph
        self.setup_db(force)


        # Go thourgh all table names and attribute, save it into a p.file
        self.create_db_graph(force)


        self.create_db_corpus(force)


        self.train_db_classifier(force)
        self.train_sql_classifier(force)

        self.config.write()
        self.comm.say("Set up complete.")


    def setup_db(self, force):
        if not force and self.config.has("DATABASE"):
            self.comm.say("Database already configured.")
            return

        self.comm.say("Configuring Database:")

        if force and self.comm.confirm("Do you want to override the current database configuration?"):
            self.config.set("DATABASE", "hostname", self.comm.ask("Enter hostname"))
            self.config.set("DATABASE", "username", self.comm.ask("Enter MySQL user"))
            self.config.set("DATABASE", "password", self.comm.ask("Enter MySQL password"))
            db_name = self.comm.ask("Enter database name")
            self.config.set("DATABASE", "database", db_name)
            self.config.write()
        else:
            if not self.config.has("DATABASE"):
                self.comm.say("Looks like there is no database setup")
                self.config.set("DATABASE", "hostname", self.comm.ask("Enter hostname"))
                self.config.set("DATABASE", "username", self.comm.ask("Enter MySQL user"))
                self.config.set("DATABASE", "password", self.comm.ask("Enter MySQL password"))
                db_name = self.comm.ask("Enter database name")
                self.config.set("DATABASE", "database", db_name)
                self.config.write()
            else:
                db_name = self.config.get("DATABASE", "database")

        try:
            database = Database(False)
        except ValueError as exception:
            self.comm.error("Error: %s" % exception)

        db_exists = False

        try:
            database.set_db(db_name)
            db_exists = True
        except ValueError as exception:
            if self.comm.confirm("Database does not exist. Create it?"):
                database.create_db(db_name)
                db_exists = True

        if db_exists:
            if self.comm.confirm("Do you want to import the database schema?"):
                database.import_schema()

            if self.comm.confirm("Do you want to seed the database?"):
                database.seed()

        self.comm.say("Database configured.")


    def create_db_graph(self, force):
        if not force and self.config.has("DATABASE", "graph_path"):
            self.comm.say("Database Graph already constructed.")
            return

        self.comm.say("Constructing Database Graph.")

        try:
            database = Database()
        except ValueError as exception:
            self.comm.error("Error: %s" % exception)

        base_path = self.config.get('PATHS', 'base')
        graph_path = os.path.join(base_path, "graph.p")


         # go thourgh the database and save the schema
        SchemaGraph().construct(database, graph_path)
        # Save the graph in a p file
        self.config.set("DATABASE", "graph_path", graph_path)
        self.comm.say("Database Graph constructed.")


    def create_db_corpus(self, force):
        if not force and self.config.has("DATABASE", "corpus_path"):
            self.comm.say("Database corpus already created.")
            return

        self.comm.say("Creating Database Corpus.")

        try:
            database = Database()
        except ValueError as exception:
            self.comm.error("Error: %s" % exception)


        "Old Version"
        base_path = self.config.get('PATHS', 'base')
        corpus_path = os.path.join(base_path, "database_corpus.p")
        gen = DBCorpusGenerator(self.config.get('PATHS', 'stanford_jar'))
        # what is that exactly: SQL statements , does not take into account the ID , but all other attributes?
        gen.create_db_corpus(database, corpus_path)
        self.config.set("DATABASE", "corpus_path", corpus_path)

        "New Version"
        # go thourgh the database and save the schema
        SchemaGraph().construct_without_id(database, corpus_path)
        # Save the graph in a p file
        self.config.set("DATABASE", "graph_path", graph_path)
        self.comm.say("Database Graph constructed.")



        self.comm.say("Database Corpus created.")


    def train_db_classifier(self, force):
        if not force and self.config.has("MODELS", "db_model"):
            self.comm.say("Database classifier already trained.")
            return

        self.comm.say("Training database classifier.")

        corpus_path = self.config.get("DATABASE", "corpus_path")

        base_path = self.config.get('PATHS', 'base')
        model_path = os.path.join(base_path, "db_model.p")

        DBCorpusClassifier().train(corpus_path, model_path)
        self.config.set("MODELS", "db_model", model_path)

        self.comm.say("Database classifier trained.")


    def train_sql_classifier(self, force):
        if not force and self.config.has("MODELS", "sql_model"):
            self.comm.say("SQL Grammar classifier already trained.")
            return

        self.comm.say("Training SQL grammar classifier.")

        base_path = self.config.get('PATHS', 'base')
        model_path = os.path.join(base_path, "sql_model.p")

        SQLGrammarClassifier().train(model_path)
        self.config.set("MODELS", "sql_model", model_path)

        self.comm.say("SQL grammar classifier trained.")
