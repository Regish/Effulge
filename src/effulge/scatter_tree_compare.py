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

from .scatter_tree_nodes import NodeType
from .scatter_tree_nodes import ListNode
from .scatter_tree_nodes import DictNode
from .scatter_tree_nodes import KeyValNode
from .scatter_tree_nodes import PrimitiveNode

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
        Type:    Any
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

def _compare_primitive_nodes(left, right, depth):
    if left.get_value() != right.get_value():
        # unequal values
        return False
    #
    # mark flags of 'left' and 'right'
    #
    if depth == 1:
        left.set_marker()
        right.set_marker()
    #
    # 'left' and 'right' have matched successfully
    #
    return True

def _compare_keyval_nodes(left, right, depth):
    are_keys_similar = _compare_nodes_func[left.get_key_reference().get_type()](
                           left.get_key_reference(),
                           right.get_key_reference(),
                           depth+1
                       )
    if are_keys_similar is False:
        # keys are different
        return False
    are_values_similar = _compare_nodes_func[left.get_value_reference().get_type()](
                             left.get_value_reference(),
                             right.get_value_reference(),
                             depth+1
                         )
    if are_values_similar is False:
        # values are different
        return False
    #
    # mark flags of 'left' and 'right'
    #
    if depth == 1:
        _mark_recursively(left)
        _mark_recursively(right)
    #
    # 'left' and 'right' have matched successfully
    #
    return True

def _compare_collection_nodes(left, right, depth):
    cache_nodes = list()
    # iterate over 'left'
    for member in left.get_members():
        #
        # member can be list node / dict node / primitive node
        #
        # search for similar match in 'right'
        probable_matches = _fetch_similar_nodes(right, member)
        if len(probable_matches) == 0:
            # didn't find a match
            return False
        # iterate over many matches
        is_matched = False
        for match in probable_matches:
            # skip if it was matched before
            if match in cache_nodes:
                continue
            # otherwise compare recursively
            if _compare_nodes_func[member.get_type()](member, match, depth+1):
                # 'left'::'member' and 'right'::'match' are equal
                # stop iterating matches
                cache_nodes.append(match)
                is_matched = True
                break
        #<------
        if is_matched is False:
            # didn't find any match
            return False
        #
        # mark flags of 'member' and last item in 'cache_nodes'
        #
        if depth == 1:
            _mark_recursively(member)
            _mark_recursively(cache_nodes[-1])
    #
    # 'left' and 'right' have matched successfully
    #
    return True

def _fetch_similar_nodes(scatter_tree, sample_node):
    # In List
    if scatter_tree.get_type() is NodeType.LIST_OR_TUPLE:
        if sample_node.get_type() in (NodeType.LIST_OR_TUPLE, NodeType.DICTIONARY):
            # filter collections
            similar_nodes = scatter_tree.filter_collection_members( sample_node.get_type(),
                                                                    sample_node.get_size()
                                                                  )
        elif sample_node.get_type() is NodeType.PRIMITIVE:
            # filter primitives
            similar_nodes = scatter_tree.filter_primitive_members(sample_node.get_value())
    # In Dict
    if scatter_tree.get_type() is NodeType.DICTIONARY:
        if sample_node.get_type() is NodeType.KEY_VALUE:
            # filter similar KV nodes
            similar_nodes = scatter_tree.filter_similar_members(sample_node)
    return similar_nodes

def _mark_recursively(scatter_tree):
    node_type = scatter_tree.get_type()
    # set marker on child nodes
    if node_type in (NodeType.LIST_OR_TUPLE, NodeType.DICTIONARY):
        for member in scatter_tree.get_members():
            _mark_recursively(member)
    elif node_type is NodeType.KEY_VALUE:
        _mark_recursively(scatter_tree.get_key_reference())
        _mark_recursively(scatter_tree.get_value_reference())
    # set marker on input node
    scatter_tree.set_marker()

# mapping node types against their respective compare function
_compare_nodes_func = {NodeType.PRIMITIVE : _compare_primitive_nodes,
                       NodeType.LIST_OR_TUPLE : _compare_collection_nodes,
                       NodeType.DICTIONARY : _compare_collection_nodes,
                       NodeType.KEY_VALUE : _compare_keyval_nodes
                      }

def check_scatter_equality(scatter_left, scatter_right):
    """
    Function to check if 2 scatter trees are equivalent.

    Parameters:
        Name: scatter_left
        Type:    scatter_tree_nodes.ListNode
                 scatter_tree_nodes.DictNode
                 scatter_tree_nodes.KeyValNode
                 scatter_tree_nodes.PrimitiveNode
        Name: scatter_right
        Type:    scatter_tree_nodes.ListNode
                 scatter_tree_nodes.DictNode
                 scatter_tree_nodes.KeyValNode
                 scatter_tree_nodes.PrimitiveNode

    Return Type:
        Boolean
    """
    if ( ( not isinstance(scatter_left, scatter_right.__class__) )
         or
         ( not isinstance( scatter_left,
                             (ListNode, DictNode, PrimitiveNode, KeyValNode)
                         )
         )
       ):
        # different input types
        # or unexpected input type
        return False
    # compare and return the outcome
    return _compare_nodes_func[scatter_left.get_type()](
                                                  scatter_left,
                                                  scatter_right,
                                                  0
                                                )

def compare_two_collections(first_collection, second_collection):
    """
    Function to compare 2 complex nested collections.
    Input Collection can be a tuple / list / dict.
    It translates the collection into a scatter tree
      and then compare the corresponding scatter trees

    Parameters:
        Name: first_collection
        Type:    tuple / list / dict
        Name: second_collection
        Type:    tuple / list / dict

    Return Type:
        Boolean
    """
    try:
        first_scatter_tree, first_node_count = scatter(first_collection)
        second_scatter_tree, second_node_count = scatter(second_collection)
        if first_node_count != second_node_count:
            # different node counts
            return False
        are_equal = check_scatter_equality(first_scatter_tree, second_scatter_tree)
        return are_equal
    except (TypeError, ValueError, OverflowError):
        return False
