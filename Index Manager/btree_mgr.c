#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include <unistd.h>

#include "btree_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "rm_data_structures.h"

BTree_Data *bTree;

Node *newLeaf(BTree_Data *bTree);

Node *newNode(BTree_Data *bTree);

Node *newTree(BTree_Data *bTree, Value *key, Node *pointer);

RC searchNodes(struct Node *pgHeader, int key, RID *result);

Node *findLeaf(Node *root, Value *key);

Node *findRecord(Node *root, Value *key);

Node *mergeNodes(BTree_Data *bTree, Node *curNode, Node *nbNode, int nbIndex, int k);

RC searchNodes(struct Node *pgHeader, int key, RID *result) {
  return RC_OK;
}

Node *newLeaf(BTree_Data *bTree) {
  Node *leaf = newNode(bTree);
  leaf->isLeaf = true;
  return leaf;
}

Node *newTree(BTree_Data *bTree, Value *key, Node *pointer) {
  Node *root = newLeaf(bTree);
  root->keys[0] = key;
  root->pointers[0] = pointer;
  root->parent = NULL;
  root->keys++;
  bTree->element++;
  return root;
}

Node *newNode(BTree_Data *treeManager) {
  treeManager->nodes++;
  Node *new_node = malloc(sizeof(Node));

  new_node->keys = malloc(5 * sizeof(Value *));  //max key = 5

  new_node->pointers = malloc(5 * sizeof(void *));

  new_node->isLeaf = false;
  new_node->keys = 0;
  new_node->parent = NULL;
  new_node->next = NULL;
  return new_node;
}

Node *findLeaf(Node *root, Value *key) {
  int i = 0;
  Node *r = root;
  while (!r->isLeaf) {
    while (i < r->keys) {
      if ((key > r->keys[i]) || (key = r->keys[i])) {
        i++;
      } else
        break;
    }
    r = (Node *) r->pointers[i];
  }
  return r;
}

Node *findRecord(Node *root, Value *key) {
  int i = 0;
  Node *n = findLeaf(root, key);
  for (i = 0; i < n->keys; i++) {
    if (n->keys[i] = key)
      break;
  }
  if (i == n->keys) {
    return NULL;
  } else {
    return (Node *) n->pointers[i];
  }
}

RC findKey(BTreeHandle *tree, Value *key, RID *result) {
  Node *node = bTree->nodePtr;
  RC search_result = searchNodes(node, key->v.intV, result);
  if (search_result != RC_OK) {
    return RC_IM_KEY_NOT_FOUND;
  }
  return RC_OK;
}

Node *insertIntoLeaf(BTree_Data *bTree, Node *leaf, Value *key, Node *pointer) {
  int i, insertion_point;

  insertion_point = 0;
  while (insertion_point < leaf->keys && (leaf->keys[insertion_point] < key))
    insertion_point++;

  leaf->keys[insertion_point] = key;
  leaf->pointers[insertion_point] = pointer;
  leaf->keys++;

  bTree->element++;
  return leaf;
}

RC getNumNodes(BTreeHandle *tree, int *result) {
  BTree_Data *btree_Data = (BTree_Data *) tree->mgmtData;
  *result = btree_Data->nodes;
  return RC_OK;
}

RC getNumEntries(BTreeHandle *tree, int *result) {
  BTree_Data *btree_Data = (BTree_Data *) tree->mgmtData;
  *result = btree_Data->keys;

  return RC_OK;
}

Node *insertIntoNode(BTree_Data *bTree, Node *parent, int left_index, Value *key, Node *right) {
  int k;
  for (k = parent->keys; k > left_index; k--) {
    parent->pointers[k + 1] = parent->pointers[k];
    parent->keys[k] = parent->keys[k - 1];
  }

  parent->pointers[left_index + 1] = right;
  parent->keys[left_index] = key;
  parent->keys++;

  return bTree->root;
}

Node *insertIntoNewRoot(BTree_Data *bTree, Node *left, Value *key, Node *right) {
  Node *root = newNode(bTree);
  return root;
}

/**
 * Merge Nodes (Note that the merge tree node refers to the implementation of the BTree data structure)
 * @param bTree
 * @param node curNode
 * @param nbNode neighborNode
 * @param neighbor_index
 * @param k
 * @return
 */
Node *mergeNodes(BTree_Data *bTree, Node *curNode, Node *nbNode, int nbIndex, int k) {

  int i, j;
  int nb_inst_index, n_end;
  Node *tmpNode;

  if (nbIndex == -1) {
    tmpNode = curNode;
    curNode = nbNode;
    nbNode = tmpNode;
  }

  nb_inst_index = nbNode->keys;
  if (!curNode->isLeaf) {
    nbNode->keys[nb_inst_index] = k;
    nbNode->keys++;

    n_end = curNode->keys;
    for (i = nb_inst_index + 1, j = 0; j < n_end; i++, j++) {
      nbNode->keys[i] = curNode->keys[j];
      nbNode->pointers[i] = curNode->pointers[j];
      nbNode->keys++;
      curNode->keys--;
    }

    nbNode->pointers[i] = curNode->pointers[j];
    for (i = 0; i < nbNode->keys + 1; i++) {
      tmpNode = (Node *) nbNode->pointers[i];
      tmpNode->parent = nbNode;
    }
  } else {
    for (i = nb_inst_index, j = 0; j < curNode->keys; i++, j++) {
      nbNode->keys[i] = curNode->keys[j];
      nbNode->pointers[i] = curNode->pointers[j];
      nbNode->keys++;
    }
    nbNode->pointers[5 - 1] = curNode->pointers[5 - 1];
  }

  free(curNode->keys);
  free(curNode->pointers);
  free(curNode);

  curNode->keys = NULL;
  curNode->pointers = NULL;
  curNode = NULL;

  return bTree->root;
}

/**
 * get KeyType
 * @param tree
 * @param result
 * @return
 */
RC getKeyType(BTreeHandle *tree, DataType *result) {
  BTree_Data *btree_Data = (BTree_Data *) tree->mgmtData;
  *result = btree_Data->keyType;
  return RC_OK;
}

/**
 * init Index Manager
 * @param mgmtData
 * @return
 */
RC initIndexManager(void *mgmtData) {
  initStorageManager();
  return RC_OK;
}

/**
 * Frees all the allocated resources
 * @return errCode
 */
RC shutdownIndexManager() {
  return RC_OK;
}

/**
 *
 * @param idxId
 * @param keyType
 * @param n
 * @return
 */
RC createBtree(char *idxId, DataType keyType, int n) {
  bTree = malloc(sizeof(BTree_Data));
  bTree->bPool = MAKE_POOL();
  bTree->pHandle = MAKE_PAGE_HANDLE();

  SM_FileHandle f;
  int resultCode;

  char pageData[PAGE_SIZE];
  memset(pageData, 0, PAGE_SIZE);

  resultCode = createPageFile(idxId);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  resultCode = openPageFile(idxId, &f);
  if (resultCode != RC_OK) {
    return resultCode;
  }

  resultCode = writeBlock(0, &f, pageData);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  // Closing File after writing
  resultCode = closePageFile(&f);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  return RC_OK;
}

/**
 * open Btree
 * @param tree
 * @param idxId
 * @return
 */
RC openBtree(BTreeHandle **tree, char *idxId) {
  int resultCode = initBufferPool(bTree->bPool, idxId, 4, RS_LRU, NULL);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  (*tree) = (BTreeHandle *) malloc(sizeof(BTreeHandle));
  (*tree)->idxId = idxId;
  (*tree)->mgmtData = bTree;
  return RC_OK;
}

/**
 * close Btree
 * @param tree
 * @return
 */
RC closeBtree(BTreeHandle *tree) {
  RC resultCode;
  resultCode = shutdownBufferPool(bTree->bPool);
  if (resultCode != RC_OK) {
    printf("closedBtree error\n");
    return resultCode;
  }
  free(tree);
  return RC_OK;
}

/**
 * delete Btree
 * @param idxId
 * @return
 */
RC deleteBtree(char *idxId) {
  RC resultCode = destroyPageFile(idxId);
  if (resultCode != RC_OK) {
    return resultCode;
  }
  return RC_OK;
}

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
  return RC_OK;
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {
  return RC_OK;
}

RC closeTreeScan(BT_ScanHandle *handle) {
  return RC_OK;
}

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
  RID *record = NULL;
  Node *leaf;
  Node *pointer;
  if (findKey(tree, key, &rid) == RC_OK) {
    return RC_IM_KEY_ALREADY_EXISTS;
  }

  if (bTree->root == NULL) {
    bTree->root = newTree(bTree, key, pointer);
    return RC_OK;
  }

  leaf = findLeaf(bTree->root, key);

  if (leaf->keys < bTree - 1) {
    leaf = insertIntoLeaf(bTree, leaf, key, pointer);
  }

  return RC_OK;

}


RC deleteKey(BTreeHandle *tree, Value *key) {
  return RC_OK;
}

/**
 * print Tree
 * @param tree
 * @return
 */
char *printTree(BTreeHandle *tree) {
  char strTree[1024];
  char *str = calloc(1024, sizeof(char));
  BTree_Data *treeData = (BTree_Data *) tree->mgmtData;
  Node *node = treeData->nodePtr;

  for (int i = 0; i < treeData->nodes; i++) {
    //todo
  }
  strcpy(strTree, str);
  free(str);
}
