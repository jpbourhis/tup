import cPickle as pickle
from nlp import Tokenizer
from database.Node import Node

class DBCorpusGenerator(object):
    """
    Generate a corpus based on the database
    """
    def __init__(self, jar_path):
        self.tokenizer = Tokenizer(jar_path)


    def create_db_corpus(self, database, path):
        print "Create_db_corpus"
        print path

        tables = {
            'campuses': ['location'],
            'courses': ['peoplesoft_course_id', 'name'],
            'faculty': ['first_name', 'last_name', 'name'],
            'sections': ['section_number'],
            'students': ['first_name', 'last_name', 'name', 'university_id', 'net_id', 'email'],
            'terms': ['semester', 'year']
        }

        corpus = []
        for table in tables:
            print "SELECT %s FROM %s" % (", ".join(tables[table]), table)

            """
            SELECT semester, year FROM terms
            SELECT location FROM campuses
            SELECT first_name, last_name, name, university_id, net_id, email FROM students
            SELECT peoplesoft_course_id, name FROM courses
            SELECT first_name, last_name, name FROM faculty
            SELECT section_number FROM sections
            """
            cursor = database.execute("SELECT %s FROM %s" % (", ".join(tables[table]), table))
            rows = cursor.fetchall()

            for row in rows:
                sentence = []
                for i, value in enumerate(row):
                    # label : students.first_name,students.last_name,students.name,Jeffrey,Watts
                    label = "%s.%s" % (table, tables[table][i])
                    if " " in str(value):
                        tokens = self.tokenizer.tokenize(value)
                        for token in tokens:
                            sentence.append((token, label))
                    else:
                        sentence.append((value, label))
                # [('Donald', 'students.first_name'), ('Murphy', 'students.last_name'), (u'Donald', 'students.name'), (u'Murphy', 'students.name'), ('N582063738', 'students.university_id'), ('dm6993', 'students.net_id'), ('dm6993@nyu.edu', 'students.email')]
                print ">>>sentence {}<<<" .format(sentence)
                corpus.append(sentence)

        pickle.dump(corpus, open(path, "wb"))

    def construct_without_id(self, database, file_path):
            print "CONSTRUCT WITHOUT ID DBCORPUS"

            """
           
            FROM SCHEMA GRAPH
            
            for (table_name,) in database.get_tables():
                table = Node(table_name)
                print "Schema Graph Construct"
                print table_name
                self.add_node(table)
    
                fields = database.get_fields(table_name)
                for field in fields:
                    attribute = Node(table_name, field[0])
    
                    print ">>>Attribute {}" .format(field[0])
                    table.add_attribute(attribute)
                    self.add_node(attribute)
    
            # link to other table name
            for table, self_key, _, foreign_table, foreign_key in database.get_foreign_keys():
                table_node = self.get_node(table)
                foreign_node = self.get_node(foreign_table)
    
                table_node.add_relation(foreign_node, self_key, foreign_key)
                foreign_node.add_relation(table_node, foreign_key, self_key)
    
            pickle.dump(self.graph_dict, open(file_path, "wb"))

            """

            # TO ADAPT FOR THE NEW VERSION
            print "Starting"
            corpus = []
            for (table_name,) in database.get_tables():
                print table_name
                fields = database.get_fields(table_name)
                fieldnames = []
                for field in fields:
                    print field[0]
                    if not field[0] == "id":
                        fieldnames.append(field[0])
                print "SELECT %s FROM %s" % (", ".join(fieldnames), table_name)

            print "Ending"


            """
            print "SELECT %s FROM %s" % (", ".join(fieldnames), table_name)

            print "SELECT %s FROM %s" % (", ".join(fields[0]), table)


                SELECT semester, year FROM terms
                SELECT location FROM campuses
                SELECT first_name, last_name, name, university_id, net_id, email FROM students
                SELECT peoplesoft_course_id, name FROM courses
                SELECT first_name, last_name, name FROM faculty
                SELECT section_number FROM sections



                cursor = database.execute("SELECT %s FROM %s" % (", ".join(tables[table]), table))
                rows = cursor.fetchall()

                for row in rows:
                    sentence = []
                    for i, value in enumerate(row):
                        # label : students.first_name,students.last_name,students.name,Jeffrey,Watts
                        label = "%s.%s" % (table, tables[table][i])
                        if " " in str(value):
                            tokens = self.tokenizer.tokenize(value)
                            for token in tokens:
                                sentence.append((token, label))
                        else:
                            sentence.append((value, label))
                    # [('Donald', 'students.first_name'), ('Murphy', 'students.last_name'), (u'Donald', 'students.name'), (u'Murphy', 'students.name'), ('N582063738', 'students.university_id'), ('dm6993', 'students.net_id'), ('dm6993@nyu.edu', 'students.email')]
                    print
                    ">>>sentence {}<<<".format(sentence)
                    corpus.append(sentence)

            pickle.dump(corpus, open(path, "wb"))
            """
