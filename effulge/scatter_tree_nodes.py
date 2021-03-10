#!/usr/bin/env python

"""
Module for representing
  - complex nested list or tuple or dictionary
  - as a Tree based data structure (termed as Scatter Tree)

Tree reprentation allows
  - equality checks on contents
  - even though the contents have different order
"""

from enum import Enum

class NodeType(Enum):
    """
    Enumeration Class for Type of Node in a Scatter Tree
    """
    PRIMITIVE = 1
    LIST_OR_TUPLE = 2
    DICTIONARY = 3
    KEY_VALUE = 4

class Marker(Enum):
    """
    Enumeration Class to Flag a Scatter Tree Node
      - GREEN, when a node is not matched yet
      - RED, when a node has been matched
    """
    RED = False
    GREEN = True

class Node:
    """
    Basic Node class.
    Actual Nodes of a Scatter tree will inherit this class.

    Structure:
        +------------+
        |    type    |
        |------------|
        |   marker   |
        +------------+
    """
    def __init__(self, node_type):
        if node_type in NodeType:
            self.type = node_type
        else:
            raise TypeError("Invalid value for 'node_type'")
        self.marker = Marker.GREEN
    def get_type(self):
        """
        Getter method for attribute 'type'
        """
        return self.type
    def get_marker(self):
        """
        Getter method for attribute 'marker'
        """
        return self.marker
    def set_marker(self):
        """
        Setter method for attribute 'marker'
        """
        if self.marker is Marker.GREEN:
            self.marker = Marker.RED
        else:
            raise ValueError("Flag is already marked. Cannot mark again.")

class PrimitiveNode(Node):
    """
    Class to denote a Node representing Primitive Datatype.

    Structure:
        +------------+
        |    type    |
        |------------|
        |   marker   |
        |------------|
        |   value    |
        +------------+
    """
    def __init__(self, value):
        super().__init__(NodeType.PRIMITIVE)
        self.value = value
    def get_value(self):
        """
        Getter method for attribute 'value'
        """
        return self.value

class CollectionNode(Node):
    """
    Class to denote a Node representing a Collection Datatype.

    Structure:
        +------------+
        |    type    |
        |------------|
        |   marker   |
        |------------|
        |    size    |
        |------------|
        |  members   |
        +------------+
    """
    def __init__(self, collectionType, size):
        if collectionType in (NodeType.LIST_OR_TUPLE, NodeType.DICTIONARY):
            super().__init__(collectionType)
        else:
            raise TypeError("Invalid value for 'collectionType'")
        self.size = size
        self.members = []
    def get_size(self):
        """
        Getter method for attribute 'size'
        """
        return self.size
    def get_members(self):
        """
        Getter method for attribute 'members'
        """
        if len(self.members) != self.size:
            raise ValueError("Attribute 'members' is incomplete")
        return self.members
    def add_member(self, node):
        """
        Setter method for attribute 'members'
        """
        if len(self.members) == self.size:
            raise OverflowError("cannot add members more than the initialized size")
        self.members.append(node)
        return True

class ListNode(CollectionNode):
    """
    Class to denote a Node representing a List or Tuple.

    Structure:
        +------------+
        |    type    |
        |------------|
        |   marker   |
        |------------|
        |    size    |
        |------------|
        |  members   |
        +------------+
    """
    def __init__(self, size):
        super().__init__(NodeType.LIST_OR_TUPLE, size)
    def filter_collection_members(self, member_type, member_length):
        """
        Method to filter selective 'members'
          which are collections with known length
        """
        if member_type not in (NodeType.LIST_OR_TUPLE, NodeType.DICTIONARY):
            raise TypeError("Invalid value for 'member_type'")
        match = [ m for m in self.get_members()
                      if m.get_type() is member_type
                          and m.get_size() == member_length
                          and m.get_marker()
                ]
        return match
    def filter_primitive_members(self, member_value):
        """
        Method to filter selective 'members'
          which are primitive with known value
        """
        match = [ m for m in self.get_members()
                      if m.get_type() is NodeType.PRIMITIVE
                          and m.get_value() == member_value
                          and m.get_marker()
                ]
        return match

class DictNode(CollectionNode):
    """
    Class to denote a Node representing a Dictionary.

    Structure:
        +------------+
        |    type    |
        |------------|
        |   marker   |
        |------------|
        |    size    |
        |------------|
        |  members   |
        +------------+
    """
    def __init__(self, size):
        super().__init__(NodeType.DICTIONARY, size)
    def filter_similar_members(self, sample_member):
        """
        Method to filter selective 'members'
          which are similar to a given key value node
        """
        #
        key_type = sample_member.get_key_reference().get_type()
        if key_type is NodeType.PRIMITIVE:
            is_key_collection = False
            key_filter = sample_member.get_key_reference().get_value()
        else:
            is_key_collection = True
            key_filter = sample_member.get_key_reference().get_size()
        #
        value_type = sample_member.get_value_reference().get_type()
        if value_type is NodeType.PRIMITIVE:
            is_value_collection = False
            value_filter = sample_member.get_value_reference().get_value()
        else:
            is_value_collection = True
            value_filter = sample_member.get_value_reference().get_size()
        #
        # filter matching for K node type and V node type
        both_types_match = [ m for m in self.get_members()
                                 if m.get_key_reference().get_type() == key_type
                                   and m.get_value_reference().get_type() == value_type
                           ]
        # filter matching for K node size or K node value
        only_key_match = [ m for m in both_types_match
                               if (  is_key_collection
                                     and m.get_key_reference().get_size() == key_filter
                                  )
                                  or (  (not is_key_collection)
                                        and m.get_key_reference().get_value() == key_filter
                                     )
                         ]
        # filter matching for V node size or V node value
        match = [ m for m in only_key_match
                      if (  is_value_collection
                            and m.get_value_reference().get_size() == value_filter
                         )
                         or (  (not is_value_collection)
                               and m.get_value_reference().get_value() == value_filter
                            )
                ]
        return match

class KeyValNode(Node):
    """
    Class to denote a Node representing a Key Value pair.

    Structure:
        +-------------------+
        |        type       |
        |-------------------|
        |       marker      |
        |-------------------|
        |   key_reference   |
        |-------------------|
        |  value_reference  |
        +-------------------+
    """
    def __init__(self):
        super().__init__(NodeType.KEY_VALUE)
        self.key_reference = None
        self.value_reference = None
    def get_key_reference(self):
        """
        Getter method for attribute 'key_reference'
        """
        return self.key_reference
    def get_value_reference(self):
        """
        Getter method for attribute 'value_reference'
        """
        return self.value_reference
    def set_key_reference(self, node):
        """
        Setter method for attribute 'key_reference'
        """
        self.key_reference = node
        return True
    def set_value_reference(self, node):
        """
        Setter method for attribute 'value_reference'
        """
        self.value_reference = node
        return True
