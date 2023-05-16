from TriplestoreQueryProcessor import *
from QueryProcessor import *
from processor import *
from data_model import *
from CollectionProcessor import *
from RelationalQueryProcessor import *
from pandas import Series, DataFrame, merge, concat
import pandas as pd


class GenericQueryProcessor(object):

    def __init__(self):
        self.queryProcessors = list()

    def cleanQueryProcessors(self):
        self.queryProcessors.clear()
        if len(self.queryProcessors) == 0:
            return True
        else:
            return False

    def addQueryProcessor(self, processor: QueryProcessor):
        try:
            self.queryProcessors.append(processor)
            return True 
        except Exception as e:
            print(e)
            return False


    def getEntitiesWithLabel(self, label):
        
        graph_db = DataFrame()
        relational_db = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_db = processor.getEntitiesWithLabel(label)
            elif isinstance(processor, RelationalQueryProcessor):
                relational_db = processor.getEntities()
            else:
                break
            
        if not graph_db.empty: #check if the call got some result
            df_joined = merge(graph_db, relational_db, left_on="id", right_on="id") #create the merge with the two db
            df_joined_fill = df_joined.fillna("") 
            grouped = df_joined_fill.groupby("id").agg({
                                                        "label": "first",
                                                        "title": "first",
                                                        "creator": lambda x: "; ".join(x)
                                                    }).reset_index() #this is to avoid duplicates when we have more than one creator
            sorted = grouped.sort_values("id") #sorted for id

            if not sorted.empty: # if the merge has some result inside, proceed
        
                result = list()

                for row_idx, row in sorted.iterrows():
                    id = row["id"]
                    label = label
                    title = row["title"]
                    creators_row = row['creator']
                    for item in creators_row: # iterate the string and find out if there are some ";", if there are, split them
                        if item == ";":
                            creators = creators_row.split(';') # if it's a string the class attribute would automatically append every string in a list
                            break
                    else:
                        creators = [creators_row] # else, create a list and the class attribute will take it directly 

                    entities = EntityWithMetadata(id, label, title, creators)
                    result.append(entities)
                

                return result
            
            else: # if the merge got no result and is empty, then take only the result of the graph_db query and fill the attributes with empty strings
                result = list()

                for row_idx, row in graph_db.iterrows():
                    id = row["id"]
                    label = label
                    title = ""
                    creators = ""

                    entities = EntityWithMetadata(id, label, title, creators)
                    result.append(entities)
                

                return result
                

        
    def getEntitieswithTitle(self, title):

        graph_db = DataFrame()
        relational_db = DataFrame()

        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_db = processor.getAllEntities()
            elif isinstance(processor, RelationalQueryProcessor):
                relational_db = processor.getEntitiesWithTitle(title)
            else:
                break        
        

        if not graph_db.empty:
            df_joined = merge(graph_db, relational_db, left_on="id", right_on="id")

            result = list()

            for row_idx, row in df_joined.iterrows():
                id = row["id"]
                label = row["label"]
                title = title
                creators = row["creator"]
                entities = EntityWithMetadata(id, label, title, creators)
                result.append(entities)

        return result
        

    def getImagesAnnotatingCanvas(self, canvasId):

        graph_db = DataFrame()
        relational_db = DataFrame()

        for processor in self.queryProcessors:

            if isinstance(processor, TriplestoreQueryProcessor):
                graph_db = processor.getEntitiesWithCanvas(canvasId)
            elif isinstance(processor, RelationalQueryProcessor):
                relational_db = processor.getAllAnnotations()
            else:
                break

        if not graph_db.empty:
            df_joined = merge(graph_db, relational_db, left_on="id", right_on="target")
            result = list()

            for row_idx, row in df_joined.iterrows():
                id = row["body"]
                images = Image(id)
            result.append(images)

        return result

    def getManifestsInCollection(self, collectionId):

        graph_db = DataFrame()
        relational_db = DataFrame()
        
        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                graph_db = processor.getManifestsInCollection(collectionId)
            elif isinstance(processor, RelationalQueryProcessor):
                relational_db = processor.getEntities()
            else:
                break
        

        if not graph_db.empty:
            df_joined = merge(graph_db, relational_db, left_on="id", right_on="id") 

            if not df_joined.empty:
               
                result = list()

                for row_idx, row in df_joined.iterrows():
                    id = row["id"]
                    label = row["label"]
                    title = row["title"]
                    creators = row["creator"]
                    items = processor.getCanvasesInManifest(id)
                    manifests = Manifest(id, label, title, creators, items)
                    result.append(manifests)

            return result
        else: 
            result = list()

            for row_idx, row in graph_db.iterrows():
                id = row["id"]
                label = row["label"]
                title = ""
                creators = ""
                items = processor.getCanvasesInManifest(id)
                manifests = Manifest(id, label, title, creators, items)
                result.append(manifests)            

            return result


        
    
# generic = GenericQueryProcessor()
# rel_qp = RelationalQueryProcessor()
# rel_qp.setDbPathOrUrl("database.db")
# rel_path = "database.db"
# rel_qp.setDbPathOrUrl(rel_path)

# grp_qp = TriplestoreQueryProcessor()
# grp_qp.setDbPathOrUrl("http://192.168.1.55:9999/blazegraph/sparql")
# grp_endpoint = "http://192.168.1.55:9999/blazegraph/sparql"
# grp_qp.setDbPathOrUrl(grp_endpoint)
# generic.addQueryProcessor(rel_qp)
# generic.addQueryProcessor(grp_qp)

# ciao = generic.getImagesAnnotatingCanvas("https://dl.ficlit.unibo.it/iiif/2/19425/canvas/p5")
# print(ciao)

# print("that contains")
# a = []
# for i in ciao:
#     a.append(vars(i)) 
# print(a)
