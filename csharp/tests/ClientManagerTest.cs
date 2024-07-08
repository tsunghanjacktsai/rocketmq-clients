/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Concurrent;
using Apache.Rocketmq.V2;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;

namespace tests
{
    [TestClass]
    public class ClientManagerTest
    {
        private const string ClientId = "fakeClientId";
        private static IClientManager CLIENT_MANAGER;
        private readonly ClientConfig clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:8080").Build();
        private static readonly Endpoints FAKE_ENDPOINTS = new Endpoints("127.0.0.1:8080");

        [TestMethod]
        public void TestHeartbeat()
        {
            CLIENT_MANAGER = new ClientManager(CreateTestClient());
            var request = new HeartbeatRequest();
            CLIENT_MANAGER.Heartbeat(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.Heartbeat(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestSendMessage()
        {
            CLIENT_MANAGER = new ClientManager(CreateTestClient());
            var request = new SendMessageRequest();
            CLIENT_MANAGER.SendMessage(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.SendMessage(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestQueryAssignment()
        {
            var request = new QueryAssignmentRequest();
            CLIENT_MANAGER.QueryAssignment(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.QueryAssignment(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestReceiveMessage()
        {
            var request = new ReceiveMessageRequest();
            CLIENT_MANAGER.ReceiveMessage(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.ReceiveMessage(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestAckMessage()
        {
            var request = new AckMessageRequest();
            CLIENT_MANAGER.AckMessage(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.AckMessage(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestChangeInvisibleDuration()
        {
            var request = new ChangeInvisibleDurationRequest();
            CLIENT_MANAGER.ChangeInvisibleDuration(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.ChangeInvisibleDuration(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestForwardMessageToDeadLetterQueue()
        {
            var request = new ForwardMessageToDeadLetterQueueRequest();
            CLIENT_MANAGER.ForwardMessageToDeadLetterQueue(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.ForwardMessageToDeadLetterQueue(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestEndTransaction()
        {
            var request = new EndTransactionRequest();
            CLIENT_MANAGER.EndTransaction(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.EndTransaction(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestNotifyClientTermination()
        {
            var request = new NotifyClientTerminationRequest();
            CLIENT_MANAGER.NotifyClientTermination(FAKE_ENDPOINTS, request, TimeSpan.FromSeconds(1));
            CLIENT_MANAGER.NotifyClientTermination(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }
        
        private Client CreateTestClient()
        {
            return new Producer(clientConfig, new ConcurrentDictionary<string, bool>(), 1, null);
        }
    }
}