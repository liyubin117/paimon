/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.client;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Client pool for using multiple clients to execute actions. */
public interface ClientPool<C, E extends Exception> {
    /** Action interface with return object for client. */
    interface Action<R, C, E extends Exception> {
        R run(C client) throws E;
    }

    /** Action interface with return void for client. */
    interface ExecuteAction<C, E extends Exception> {
        void run(C client) throws E;
    }

    <R> R run(Action<R, C, E> action) throws E, InterruptedException;

    void execute(ExecuteAction<C, E> action) throws E, InterruptedException;

    /** Default implementation for {@link ClientPool}. */
    abstract class ClientPoolImpl<C, E extends Exception> implements Closeable, ClientPool<C, E> {

        private volatile LinkedBlockingDeque<C> clients;

        /**
         * 初始化阶段，避免了每次操作都建立数据库连接的开销，且同一个连接可以被多次使用，限制同时运行的连接数，且不互相影响：
         *  创建ClientPool时，指定连接池大小（如20个连接）
         *  构造函数会立即创建指定数量的数据库连接
         *  所有连接都被放入LinkedBlockingDeque队列中
         */
        protected ClientPoolImpl(int poolSize, Supplier<C> supplier) {
            this.clients = new LinkedBlockingDeque<>();
            for (int i = 0; i < poolSize; i++) {
                this.clients.add(supplier.get()); // 预创建所有连接并放入队列，supplier由子类定义
            }
        }

        /**
         * run方法只是从队列获取客户端，而不是直接连接数据库
         * 使用阶段：
         *  当调用run方法时，从队列头部获取一个可用连接
         *  执行用户提供的Action操作（如执行SQL）
         *  操作完成后，无论成功还是失败，都会将连接归还到队列头部
         */
        //
        @Override
        public <R> R run(Action<R, C, E> action) throws E, InterruptedException {
            while (true) {
                LinkedBlockingDeque<C> clients = this.clients; // 线程安全
                if (clients == null) {
                    throw new IllegalStateException("Cannot get a client from a closed pool");
                }
                C client = clients.pollFirst(10, TimeUnit.SECONDS); // 关键点1：从队列获取客户端，最多等10秒
                if (client == null) {
                    continue;
                }
                try {
                    return action.run(client); // 关键点2：执行用户提供的操作
                } finally {
                    clients.addFirst(client); // 关键点3：用完后归还客户端
                }
            }
        }

        @Override
        public void execute(ExecuteAction<C, E> action) throws E, InterruptedException {
            run(
                    (Action<Void, C, E>)
                            client -> {
                                action.run(client);
                                return null;
                            });
        }

        protected abstract void close(C client);

        @Override
        public void close() {
            LinkedBlockingDeque<C> clients = this.clients;
            this.clients = null;
            if (clients != null) {
                List<C> drain = new ArrayList<>();
                clients.drainTo(drain);
                drain.forEach(this::close);
            }
        }
    }
}
