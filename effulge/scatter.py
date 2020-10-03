#!/usr/bin/env python

"""
Module for
    1. Translating complex nested datatype
       into a Tree like structure (termed as Scatter Tree)
    2. Comparing two scatter trees for equality

Tree based comparison allows
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
    Ennumeration Class to Flag a Scatter Tree Node
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
        self.marker = Marker.RED

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

def _scatter(iterable_object, node_count=0):
    """
    For internal use only.
    Users are discouraged to invoke this function directly.

    Function to create a scatter tree from an object.
    It recursively parses the given 'iterable_object'
      - creates a node for each element
      - builds the scatter tree
      - it also tracks the total number of nodes.

    Parameters:
        Name:  iterable_object
        Tyoe:    Any
        Name:  node_count    (OPTIONAL)
        Type:    int         (default is 0)

    Return Type:
       tuple(object_reference, int)
           'object_reference' points to Root Node of the Scatter Tree
           'int' value denotes the total node count in the Scatter Tree
    """
    if isinstance(iterable_object, (list, tuple)):
        # scatter list/tuple into many nodes
        list_node = ListNode(len(iterable_object))
        node_count += 1
        for m_obj in iterable_object:
            m_node, node_count = _scatter(m_obj, node_count)
            list_node.add_member(m_node)
        root_node = list_node
    elif isinstance(iterable_object, dict):
        # scatter dict into many nodes
        dict_node = DictNode(len(iterable_object))
        node_count += 1
        for (k_obj,v_obj) in iterable_object.items():
            k_node, node_count = _scatter(k_obj, node_count)
            v_node, node_count = _scatter(v_obj, node_count)
            kv_node = KeyValNode()
            node_count += 1
            kv_node.set_key_reference(k_node)
            kv_node.set_value_reference(v_node)
            dict_node.add_member(kv_node)
        root_node = dict_node
    else:
        # retain as single node
        primitive_node = PrimitiveNode(iterable_object)
        node_count += 1
        root_node = primitive_node
    return (root_node, node_count)

def scatter(iterable_object):
    """
    Function to create a scatter tree from an iterable object.

    Parameters:
        Name:  iterable_object
        Tyoe:    Any

    Return Type:
       tuple(object_reference, int)
           'object_reference' points to Root Node of the Scatter Tree
           'int' value denotes the total node count in the Scatter Tree
    """
    scatter_tree, total_node_count = _scatter(iterable_object)
    return (scatter_tree, total_node_count)
