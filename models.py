from pymongo import MongoClient
from pymongo.collection import Collection as MongoCollection

import json
from bson import ObjectId
import re
import pandas as pd

class Document:
    """
    Document class to interact with MongoDB documents
    """
    def __init__(self, document: dict, ids: list):
        self.content = document
        if len(ids) == 1:
            # Check if the id exists in content before accessing
            if ids[0] in self.content:
                self.id = self.content[ids[0]].replace(" ", "_") if isinstance(self.content[ids[0]], str) else str(self.content[ids[0]])
            else:
                # Generate a new ObjectId and use it as the id
                new_id = ObjectId()
                self.content['_id'] = new_id
                self.id = str(new_id)
        else:
            self.id = "".join([self.content[id] for id in ids if id in self.content]).replace(" ", "_")
        
        for key in self.content:
            # If the key is a str and can be converted to an ObjectId, convert it
            if isinstance(self.content[key], str) and bool(re.match("^[a-f0-9]{24}$", self.content[key])):
                self.content[key] = ObjectId(self.content[key])

    def __repr__(self) -> str:
        return str(self.id)

    def __str__(self) -> str:
        return f"Document: {str(self.content['_id'])}\n" + "\n".join([f"{str(key).replace('_', ' ').title()}: {self.content[key], type(self.content[key])}" for key in self.content if key != '_id'])
    
    def __eq__(self, other) -> bool:
        if isinstance(other, Document):
            for key in self.content:
                if key in ['created_at', 'updated_at', 'createdAt', 'updatedAt', '_id']:
                    continue
                if key not in other.content:
                    print(f"WARNING: Key '{key}' not in '{other.id}' document.")
                    continue
                if str(self.content[key]).lower() != str(other.content[key]).lower():
                    print(f"WARNING: Key '{key}': '{self.id}': '{self.content[key]}' != '{other.id}': '{other.content[key]}' documents.")
                    return False
            return True
        print(f"Object '{other}' is not a Document.")
        return False

class Collection:
    """
    Collection class to interact with MongoDB collections
    """
    def __init__(self, collection: str, docs: ('MongoCollection' or "Collection" or None or list), ids: list = ['_id'], skip_values: dict = {}):
        self.name = collection
        self.skip_values = skip_values
        if docs is None:
            self.documents = []
        elif isinstance(docs, MongoCollection):
            self.documents = [Document(doc, ids) for doc in docs.find()]
        elif isinstance(docs, self.__class__):
            self.documents = docs.documents
        elif isinstance(docs, list):
            if isinstance(docs[0], Document):
                self.documents = docs
            elif isinstance(docs[0], dict):
                self.documents = [Document(doc, ids) for doc in docs]
            else:
                raise ValueError(f"Invalid type '{type(docs[0])}' for 'docs' parameter. Must be 'Document' or 'dict'.")
        else:
            raise ValueError(f"Invalid type '{type(docs)}' for 'docs' parameter. Must be 'MongoCollection', 'Collection', 'List', or 'None'.")

    def set_ids(self, ids: list) -> None:
        self.documents = [Document(doc.content, ids) for doc in self.documents]

    def __repr__(self) -> str:
        return f"Collection('{self.name}', {len(self.documents)})"

    def __str__(self) -> str:
        return f"Collection: {self.name}\nDocuments: {len(self.documents)}"
    
    def __getattr__(self, key) -> Document:
        document_map = {doc.id: doc for doc in self.documents if doc.id == key}
        if key in document_map:
            return document_map[key]
        raise AttributeError(f"No document with ID '{key}' in the collection.")
    
    def __contains__(self, item) -> bool:
            # Customize this method to define the behavior of "in"
            not_in = []

            for doc in item.documents:
                # Check if the document should be skipped based on skip_values
                skip = False
                for key, values in item.skip_values.items():
                    if key in doc.content and doc.content[key] in values:
                        skip = True
                        break

                if skip:
                    continue

                if [doc] == [doc2 for doc2 in self.documents if doc.id == doc2.id]:
                    continue
                print(f"WARNING: Document '{doc.id}' not in '{item.name}' collection.")
                not_in.append(doc.content)

            if not_in == []:
                return True

            with open('not_in.json', 'w') as f:
                for dicts in not_in:
                    for keys in dicts:
                        dicts[keys] = str(dicts[keys])
                json.dump(not_in, f)
            return False
    
    def __getitem__(self, fields: list) -> 'Collection':
        """
        Return a new Collection object containing only the specified fields for each document.
        """
        # Ensure the provided fields is a list
        if not isinstance(fields, list):
            raise ValueError("Fields should be provided as a list")

        # Extract the subset of fields from each document
        sliced_docs = [{field: doc.content[field] for field in fields if field in doc.content} for doc in self.documents]
        
        # Create and return a new Collection object with the sliced documents
        return Collection(self.name, sliced_docs)
    
    def __add__(self, other: "Collection") -> "Collection":
        """
        Define behavior for the + operator. Combines the current collection with another.
        """
        if not isinstance(other, Collection):
            raise ValueError("Can only add instances of Collection together")

        combined_name = self.name + "_and_" + other.name

        # Extract _id values from both collections
        self_ids = [doc.content.get('_id') for doc in self.documents]
        other_ids = [doc.content.get('_id') for doc in other.documents]

        # Check for conflicting _id values
        conflicting_ids = set(self_ids).intersection(other_ids)
        if conflicting_ids:
            raise ValueError(f"Conflicting _id values found: {conflicting_ids}\nConsider using remove_ids() to remove _id values from one of the collections.")

        # Combine the documents of the two collections
        combined_documents = self.documents + other.documents

        # Return the combined collection
        return Collection(combined_name, combined_documents)
    
    def remove_ids(self, in_place: bool = False) -> None or "Collection":
        new_documents = []
        for doc in self.documents:
            new_doc = doc.content
            new_doc.pop('_id', None)
            new_documents.append(new_doc)
        if in_place:
            self.documents = new_documents
        else:
            return Collection(self.name, new_documents)
        
    def transform(self, new_name: str, column: str, func: callable, in_place: bool = False) -> None or "Collection":
        """
        Transform all values in a column with a function and add the transformed column to the collection
        """
        new_documents = []

        for doc in self.documents:
            new_doc_content = doc.content.copy()  # Creating a copy to avoid modifying the original content
            
            if column in new_doc_content:
                new_doc_content[new_name] = func(new_doc_content[column])
            else:
                print(f"WARNING: Column '{column}' not found in document ID {doc.id}.")
                new_doc_content[new_name] = "UNAVAILABLE"  # or continue, depending on desired behavior
            
            # Use the ids that you originally used to create the Document objects in the Collection
            new_documents.append(Document(new_doc_content, doc.id)) 

        if in_place:
            self.documents = new_documents
        else:
            return Collection(self.name, new_documents)
        
    def add_document(self, doc: dict) -> None:
        """
        Add a document to the collection
        """
        self.documents.append(Document(doc, ['_id']))

    def resolve(self, other: "Collection", mapping: dict, in_place: bool = False) -> None or "Collection":
        """
        Resolve values in the self collection based on a mapping from the other collection.
        """
        # Create a dictionary for faster lookups from the 'other' collection
        lookup = {doc.content["_id"]: doc.content for doc in other.documents}

        new_documents = []

        for doc in self.documents:
            new_doc_content = doc.content.copy()
            for key_self, key_other in mapping.items():
                # Check if the key from self exists in the document
                if key_self in new_doc_content:
                    lookup_id = new_doc_content[key_self]
                    # Check if the corresponding value exists in the 'other' collection
                    if lookup_id in lookup:
                        # Replace the value
                        new_doc_content[key_self] = lookup[lookup_id].get(key_other)

            # Append the new document
            new_documents.append(Document(new_doc_content, doc.id))

        if in_place:
            self.documents = new_documents
        else:
            return Collection(self.name, new_documents)

    def filter_by(self, conditions: dict, in_place: bool = False) -> None or "Collection":
        """
        Filters the collection based on the provided conditions.
        """
        def matches_conditions(document: Document) -> bool:
            """
            Check if a document meets all conditions.
            """
            for key, value in conditions.items():
                if callable(value):
                    if key not in document.content or not value(document.content[key]):
                        return False
                else:
                    if key not in document.content or document.content[key] != value:
                        return False
            return True
        # Filtering documents
        filtered_documents = [doc for doc in self.documents if matches_conditions(doc)]
        if in_place:
            self.documents = filtered_documents
        else:
            return Collection(self.name, filtered_documents)

    def to_df(self) -> pd.DataFrame:
        """
        Converts the Collection's documents to a Pandas DataFrame.
        """
        # Extracting the content of each Document to create the DataFrame
        df = pd.DataFrame([doc.content for doc in self.documents])
        return df

    def to_excel(self, file_path: str, sort_date: bool = False) -> None:
        """
        Exports the Collection's documents to an Excel file.
        """
        df = self.to_df()
        if sort_date:
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
            df = df.sort_values(by='date')
            df['date'] = df['date'].dt.strftime('%m/%d/%Y')
        if '_id' in df.columns:
            df.drop('_id', axis=1, inplace=True)
        # Saving the DataFrame to an Excel file
        df.to_excel(file_path, engine='openpyxl', index=False)

class Database:
    """
    Database class to connect to interact with MongoDB
    """
    def __init__(self, connection: str, database: str, skip_collections: list = ['files']):
        self._connection = connection
        self._database = database
        self._skip_collections = skip_collections
        self._load_database()

    def _load_database(self) -> None:
        print(f"[!][{self._database}]: Connecting to database...")
        self.client = MongoClient(self._connection)
        print(f"[!][{self._database}]: Connected to database successfully.")
        self.db = self.client[self._database]
        print(f"[!][{self._database}]: Loading collections...")
        self.collections = [Collection(collection, self.db[collection]) for collection in self.db.list_collection_names() if collection not in self._skip_collections]
        print(f"[!][{self._database}]: Loaded {len(self.collections)} collections successfully.")

    def __str__(self) -> str:
        return f"Database: {self.db.name}\nCollections: {self.collections}"
    
    def __eq__(self, other) -> bool:
        if isinstance(other, Database):
            return self.db.name == other.db.name
        return False
    
    def __getattr__(self, key) -> Collection:
        document_map = {key: collection for collection in self.collections if collection.name == key}
        if key in document_map:
            return document_map[key]
        raise AttributeError(f"No document with ID '{key}' in the collection.")
    
    def refresh(self) -> None:
        """
        WARNING: This method will delete all collections in the database object 
        and replace them with the collections in the live MongoDB database!
        Make sure you have a copy of the data, or the data has been made live, before calling this method.

        Refresh the collections in the database
        """
        self._load_database()
    
    def make_live(self, collection: "Collection", overwrite: bool = False) -> None:
        if collection.documents == []:
            raise ValueError(f"Collection '{collection.name}' has no documents. Cannot make live.")
        if collection.name in self.db.list_collection_names() and not overwrite:
            raise ValueError(f"Collection '{collection.name}' already exists in the live database. To update, set the 'overwrite' parameter to True.")
        if collection.name in self.db.list_collection_names() and overwrite:
            self.db.drop_collection(collection.name)
        self.db.create_collection(collection.name)
        mongo_collection = self.db[collection.name]
        mongo_collection.insert_many([doc.content for doc in collection.documents])

    def add_collection(self, collection: Collection, make_live: bool = False) -> None:
        """
        Add a given collection to the database's collection list.
        Optionally updates the live MongoDB database to reflect the added collection.
        """
        # Ensure there isn't already a collection with the same name
        for existing_collection in self.collections:
            if existing_collection.name == collection.name:
                raise ValueError(f"A collection named {collection.name} already exists in the database")

        # Add the new collection to the internal list
        self.collections.append(collection)

        # If update_live is True, create the new collection in the live MongoDB database
        if make_live:
            self.make_live(collection)

    def new_collection(self, name: str) -> Collection:
        """
        Add a given collection to the database's collection list.
        Optionally updates the live MongoDB database to reflect the added collection.
        """
        # Ensure there isn't already a collection with the same name
        for existing_collection in self.collections:
            if existing_collection.name == name:
                raise ValueError(f"A collection named {name} already exists in the database")

        # Add the new collection to the internal list
        collection = Collection(name, None)
        self.collections.append(collection)

        return collection
