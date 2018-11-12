import cPickle as pickle
from database.Node import Node

# new entry from nlp import Tokenizer


class SchemaGraph(object):
    def __init__(self, file_path1=None):
        if file_path is None:
            self.graph_dict = {}
        else:
            self.graph_dict = pickle.load(open(file_path1, "rb"))


    def get_node(self, label):
        return self.graph_dict[label]


    def nodes(self, type=None):
        if type is None:
            return self.graph_dict.keys()

        return [node.label for node in self.graph_dict.values() if node.type == type]


    def add_node(self, node):
        if node.label not in self.graph_dict:
            self.graph_dict[node.label] = node

    def get_direct_path(self, table_name_a, table_name_b):
        queue = [(table_name_a, [])]
        while queue:
            (vertex, path) = queue.pop(0)
            node = self.get_node(vertex)
            for next in node.relations:
                if next[0] == table_name_b:
                    return path + [next]
                else:
                    queue.append((next[0], path + [next]))

    def construct(self, database, file_path):
        print file_path

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

    def construct_without_id(self, database, file_path,jar_path):
        mytokenizer = jar_path

        """
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
            
        """

        #TO ADAPT FOR THE NEW VERSION
        corpus = []
        for (table_name,) in database.get_tables():
            fields = database.get_fields(table_name)
            fieldnames = []
            for field in fields:
                if not field[0] == "id":
                    fieldnames.append(field[0])
            print "SELECT %s FROM %s" % (", ".join(fieldnames), table_name)



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