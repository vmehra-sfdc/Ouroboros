/**
 * Copyright (c) 2008 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.salesforce.ouroboros.util.lockfree;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * This is an implementation of a lock-free FIFO queue data structure. The
 * implementation is according to the paper An Optimistic Approach to Lock-Free
 * FIFO Queues by Edya Ladan-Mozes and Nir Shavit, 2004. To gain a complete
 * understanding of this data structure, please first read this paper, available
 * at: http://www.springerlink.com/content/h84dfexjftdal4p4/
 * <p>
 * First-in-first-out (FIFO) queues are among the most fundamental and highly
 * studied concurrent data structures. The most effective and practical
 * dynamic-memory concurrent queue implementation in the literature is the
 * lock-free FIFO queue algorithm of Michael and Scott, included in the standard
 * Java Concurrency Package. This paper presents a new dynamic-memory lock-free
 * FIFO queue algorithm that performs consistently better than the Michael and
 * Scott queue.
 * <p>
 * The key idea behind our new algorithm is a novel way of replacing the
 * singly-linked list of Michael and Scott, whose pointers are inserted using a
 * costly compare-and-swap (CAS) operation, by an optimistic doubly-linked list
 * whose pointers are updated using a simple store, yet can be fixed if a bad
 * ordering of events causes them to be inconsistent. It is a practical example
 * of an optimistic approach to reduction of synchronization overhead in
 * concurrent data structures.
 * <p>
 * Sample performance results here.
 * <p>
 * The following operations are thread-safe and scalable (but see notes in
 * method javadoc): first, offer, poll, peek
 * <p>
 * The following operations are not thread-safe: size, iterator
 * 
 * @author Xiao Jun Dai
 * 
 * @param <E>
 *            type of element in the queue
 */
public class LockFreeQueue<E> extends AbstractQueue<E> implements Queue<E> {
    /**
     * Internal node definition of queue.
     * 
     * @param <E>
     *            type of element in node
     */
    private static class Node<E> {
        // next pointer point to next node in the queue
        // prev pointer point to previous node in the queue
        Node<E> next, prev;

        // value stored in the node
        E       value;

        /**
         * default constructor.
         */
        public Node() {
            value = null;
            next = prev = null;
        }

        /**
         * constructor with default value.
         * 
         * @param val
         *            default value
         */
        public Node(E val) {
            value = val;
            next = prev = null;
        }

        /**
         * get value of next pointer.
         * 
         * @return next node
         */
        public Node<E> getNext() {
            return prev;
        }
    }

    /**
     * iterator definition of queue.
     * 
     */
    private class QueueItr implements Iterator<E> {
        /**
         * value in the next n.
         */
        private E       nextItem;
        /**
         * next node.
         */
        private Node<E> nextNode;

        /**
         * default constructor.
         */
        QueueItr() {
            advance();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return nextNode != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public E next() {
            if (nextNode == null) {
                throw new NoSuchElementException();
            }
            return advance();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Moves to next valid node and returns item to return for next(), or
         * null if no such.
         * 
         * @return value of next node
         */
        private E advance() {
            // value of next node
            E x = nextItem;

            // p point to next valid node
            Node<E> p = nextNode == null ? first() : nextNode.getNext();
            while (true) {
                // reach the end
                if (p == null) {
                    nextNode = null;
                    nextItem = null;
                    return x;
                }
                E item = p.value;
                if (item != null) {
                    // p is a valid node
                    nextNode = p;
                    nextItem = item;
                    return x;
                } else {
                    // skip over nulls
                    p = p.getNext();
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<LockFreeQueue, Node> HEAD_UPDATER     = AtomicReferenceFieldUpdater.newUpdater(LockFreeQueue.class,
                                                                                                                                    Node.class,
                                                                                                                                    "head");
    private static final boolean                                          IF_BACKOFF       = false;

    /*
     * tailUpater and headUpdater is used with AtomicReferenceFieldUpdater to
     * update its value atomiclly
     */
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<LockFreeQueue, Node> TAIL_UPDATER     = AtomicReferenceFieldUpdater.newUpdater(LockFreeQueue.class,
                                                                                                                                    Node.class,
                                                                                                                                    "tail");

    private static final int                                              WAIT_FOR_BACKOFF = 10;
    private Node<E>                                                       dummy;

    // head and tail pointer of queue
    private volatile Node<E>                                              head;

    private volatile Node<E>                                              tail;

    /**
     * default constructor.
     */
    public LockFreeQueue() {
        dummy = new Node<E>();
        head = dummy;
        tail = dummy;
    }

    /**
     * Constructor with initial collection.
     * 
     * @param c
     *            collection for initialization
     */

    public LockFreeQueue(Collection<? extends E> c) {
        addAll(c);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return head.value == null && tail.value == null;
        // or return first() == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<E> iterator() {
        return new QueueItr();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean offer(E e) {
        /*
         * To insert a value, the enqueue method creates a new node that
         * contains the value, and then tries to insert this node to the queue.
         * As seen in Figure 2 in the paper, the enqueue reads the current tail
         * of the queue, and sets the new node's next pointer to point to that
         * same node. Then it tries to atomically modify the tail to point to
         * its new node using a CAS operation. If the CAS succeeded, the new
         * node was inserted into the queue. Otherwise the enqueue retries.
         */
        if (e == null) {
            throw new IllegalArgumentException();
        }

        Node<E> node = new Node<E>(e);
        while (true) {
            Node<E> tail = this.tail;
            node.next = tail;
            if (casTail(tail, node)) {
                // Thread.yield();
                tail.prev = node;
                return true;
            }

            if (IF_BACKOFF) {
                // back off, wait for 10 milliseconds before retry.
                try {
                    Thread.sleep(WAIT_FOR_BACKOFF);
                } catch (Exception exp) {
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E peek() {
        while (true) {
            Node<E> header = this.head;
            if (header.value != null) {
                return header.value;
            }

            Node<E> tail = this.tail;

            if (header == this.head) {
                /*
                 * In our algorithm, a dummy node is a special node with a dummy
                 * value. It is created and inserted to the queue when it
                 * becomes empty as explained above. Since a dummy node does not
                 * contain a real value, it must be skipped when nodes are
                 * deleted from the queue. The steps for skipping a dummy node
                 * are similar to those of a regular dequeue, except that no
                 * value is returned. When a dequeue method identifies that the
                 * head points to a dummy node and the tail does not, as in
                 * Figure 6 Part B in the paper, it modifies the head using a
                 * CAS to point to the node pointed by the prev pointer of this
                 * dummy node. Then it can continue to dequeue nodes.
                 */
                if (tail == header) {
                    return null;
                } else {
                    Node<E> fstNodePrev = header.prev;
                    if (null == fstNodePrev) {
                        fixList(tail, header);
                        continue;
                    }
                    casHead(header, fstNodePrev);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E poll() {
        // comment is similiar to first()
        // TODO refactor duplicate code with first()
        Node<E> tail, head, fstNodePrev;
        E val;
        while (true) {
            head = this.head;
            tail = this.tail;
            fstNodePrev = head.prev;
            val = head.value;
            if (head == this.head) {
                if (val != null) { /* head val is dummy? */
                    if (tail != head) { // more than 1 node
                        if (null != fstNodePrev) {
                            if (casHead(head, fstNodePrev)) {
                                fstNodePrev.next = null;
                                return val;
                            }
                        } else {
                            fixList(tail, head);
                            continue;
                        }
                    } else { // Last Node in the queue, Figure 6.A
                        dummy.next = tail;
                        dummy.prev = null;
                        if (casTail(tail, dummy)) {
                            head.prev = dummy;
                        }
                        continue;
                    }
                } else { // head points to dummy, Figure 6.B
                    if (tail == head) {
                        return null;
                    } else {
                        if (null != fstNodePrev) {
                            casHead(head, fstNodePrev);
                        } else {
                            fixList(tail, head);
                        }
                    }
                }
            }

            if (IF_BACKOFF) {
                try {
                    Thread.sleep(WAIT_FOR_BACKOFF);
                } catch (Exception exp) {
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public int size() {
        // size is computed on the fly when it is called since size() is assumed
        // to be
        // called infrequently. If not, use a atomic int to record the size and
        // update it whenever offer and poll.
        int count = 0;
        Node<E> cur;
        for (cur = first(); cur != null && cur.value != null; cur = cur.prev) {
            if (++count == Integer.MAX_VALUE) {
                break;
            }
        }
        return count;
    }

    /**
     * update head's value atomically using compare and swap.
     * 
     * @param cmp
     *            expected value
     * @param val
     *            new value
     * @return true if cas is successful, otherwise false
     */
    private boolean casHead(Node<E> cmp, Node<E> val) {
        return HEAD_UPDATER.compareAndSet(this, cmp, val);
    }

    /**
     * update tail's value atomically using compare and swap.
     * 
     * @param cmp
     *            expected value
     * @param val
     *            new value
     * @return true if cas is successful, otherwise false
     */
    private boolean casTail(Node<E> cmp, Node<E> val) {
        return TAIL_UPDATER.compareAndSet(this, cmp, val);
    }

    /**
     * get first node of queue.
     * 
     * @return first node of queue
     */
    private Node<E> first() {
        while (true) {
            Node<E> header = this.head;
            if (header.value != null) {
                return header;
            }

            Node<E> tail = this.tail;

            if (header == this.head) {
                /*
                 * In our algorithm, a dummy node is a special node with a dummy
                 * value. It is created and inserted to the queue when it
                 * becomes empty as explained above. Since a dummy node does not
                 * contain a real value, it must be skipped when nodes are
                 * deleted from the queue. The steps for skipping a dummy node
                 * are similar to those of a regular dequeue, except that no
                 * value is returned. When a dequeue method identifies that the
                 * head points to a dummy node and the tail does not, as in
                 * Figure 6 Part B in the paper, it modifies the head using a
                 * CAS to point to the node pointed by the prev pointer of this
                 * dummy node. Then it can continue to dequeue nodes.
                 */
                if (tail == header) {
                    return null;
                } else {
                    Node<E> fstNodePrev = header.prev;
                    if (null == fstNodePrev) {
                        fixList(tail, header);
                        continue;
                    }
                    casHead(header, fstNodePrev);
                }
            }
        }
    }

    /**
     * fix the list if a bad ordering of events causes them to be inconsistent.
     * 
     * If a prev pointer is found to be inconsistent, we run a fixList method
     * along the chain of next pointers which is guaranteed to be consistent.
     * Since prev pointers become inconsistent as a result of long delays, not
     * as a result of contention, the frequency of calls to fixList is low.
     * 
     * @param tail
     *            tail node
     * @param head
     *            head node
     */
    private void fixList(Node<E> tail, Node<E> head) {
        /*
         * set current node to tail. The chain of next pointers is guaranteed to
         * be consistent. Fix the inconsistent previous pointers from tail to
         * head.
         */
        Node<E> curNode = tail;
        while (head == this.head && curNode != head) {
            Node<E> curNodeNext = curNode.next;
            if (curNodeNext == null) {
                break;
            }
            Node<E> nextNodePrev = curNodeNext.prev;

            // Fix the inconsistent previous pointers
            if (nextNodePrev != curNode) {
                curNodeNext.prev = curNode;
            }
            curNode = curNodeNext;
        }
    }
}
