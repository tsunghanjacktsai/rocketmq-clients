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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class TransactionTest
    {
        [TestMethod]
        public void TestTryAddMessage()
        {
            var producer = CreateTestClient();
            var transaction = new Transaction(producer);
            var bytes = Encoding.UTF8.GetBytes("fakeBytes");
            const string tag = "fakeTag";
            var message = new Message.Builder()
                .SetTopic("fakeTopic")
                .SetBody(bytes)
                .SetTag(tag)
                .SetKeys("fakeMsgKey")
                .Build();
            var publishingMessage = transaction.TryAddMessage(message);
            Assert.AreEqual(MessageType.Transaction, publishingMessage.MessageType);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestTryAddExceededMessages()
        {
            var producer = CreateTestClient();
            var transaction = new Transaction(producer);
            var bytes = Encoding.UTF8.GetBytes("fakeBytes");
            const string tag = "fakeTag";
            var message0 = new Message.Builder()
                .SetTopic("fakeTopic")
                .SetBody(bytes)
                .SetTag(tag)
                .SetKeys("fakeMsgKey")
                .Build();
            transaction.TryAddMessage(message0);
            var message1 =  new Message.Builder()
                .SetTopic("fakeTopic")
                .SetBody(bytes)
                .SetTag(tag)
                .SetKeys("fakeMsgKey")
                .Build();
            transaction.TryAddMessage(message1);
        }
        
        [TestMethod]
        public void TestTryAddReceipt()
        {
            var producer = CreateTestClient();
            var transaction = new Transaction(producer);
            var bytes = Encoding.UTF8.GetBytes("fakeBytes");
            const string tag = "fakeTag";
            var message = new Message.Builder()
                .SetTopic("fakeTopic")
                .SetBody(bytes)
                .SetTag(tag)
                .SetKeys("fakeMsgKey")
                .Build();
            var publishingMessage = transaction.TryAddMessage(message);
            var mq0 = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = "127.0.0.1",
                                Port = 8080
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "foo-bar-namespace",
                    Name = "testTopic",
                }
            };
            var metadata = producer.Sign();
            var sendResultEntry = new Proto.SendResultEntry
            {
                MessageId = "fakeMsgId",
                TransactionId = "fakeTxId",
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Offset = 1
            };
            var sendMessageResponse = new Proto.SendMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Entries = { sendResultEntry }
            };
            var invocation = new RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>(null, sendMessageResponse, metadata);
            var sendReceipt = SendReceipt.ProcessSendMessageResponse(new MessageQueue(mq0), invocation);
            transaction.TryAddReceipt(publishingMessage, sendReceipt.GetEnumerator().Current);
        }
        
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestTryAddReceiptNotContained()
        {
            var producer = CreateTestClient();
            var transaction = new Transaction(producer);
            var bytes = Encoding.UTF8.GetBytes("fakeBytes");
            const string tag = "fakeTag";
            var message = new Message.Builder()
                .SetTopic("fakeTopic")
                .SetBody(bytes)
                .SetTag(tag)
                .SetKeys("fakeMsgKey")
                .Build();
            var publishingMessage = new PublishingMessage(message, new PublishingSettings("fakeNamespace",
                "fakeClientId", new Endpoints("fakeEndpoints"), new Mock<IRetryPolicy>().Object,
                TimeSpan.FromSeconds(10), new ConcurrentDictionary<string, bool>()), true);
            var mq0 = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = "127.0.0.1",
                                Port = 8080
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "foo-bar-namespace",
                    Name = "TestTopic",
                }
            };
            var metadata = producer.Sign();
            var sendResultEntry = new Proto.SendResultEntry
            {
                MessageId = "fakeMsgId",
                TransactionId = "fakeTxId",
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Offset = 1
            };
            var sendMessageResponse = new Proto.SendMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Entries = { sendResultEntry }
            };
            var invocation = new RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>(null, sendMessageResponse, metadata);
            var sendReceipt = SendReceipt.ProcessSendMessageResponse(new MessageQueue(mq0), invocation);
            transaction.TryAddReceipt(publishingMessage, sendReceipt.GetEnumerator().Current);
        }
        
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestCommitWithNoReceipts()
        {
            var producer = CreateTestClient();
            var transaction = new Transaction(producer);
            await transaction.Commit();
        }
        
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestRollbackWithNoReceipts()
        {
            var producer = CreateTestClient();
            var transaction = new Transaction(producer);
            await transaction.Rollback();
        }
        
        [TestMethod]
        public async Task TestCommit()
        {
            var producer = CreateTestClient();
            var transaction = new Transaction(producer);
            var bytes = Encoding.UTF8.GetBytes("fakeBytes");
            const string tag = "fakeTag";
            var message = new Message.Builder()
                .SetTopic("fakeTopic")
                .SetBody(bytes)
                .SetTag(tag)
                .SetKeys("fakeMsgKey")
                .Build();
            var publishingMessage = transaction.TryAddMessage(message);
            var mq0 = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = "127.0.0.1",
                                Port = 8080
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "foo-bar-namespace",
                    Name = "TestTopic",
                }
            };
            var metadata = producer.Sign();
            var sendResultEntry = new Proto.SendResultEntry
            {
                MessageId = "fakeMsgId",
                TransactionId = "fakeTxId",
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Offset = 1
            };
            var sendMessageResponse = new Proto.SendMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Entries = { sendResultEntry }
            };
            var sendMessageInvocation = new RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>(null,
                sendMessageResponse, metadata);
            var sendReceipt = SendReceipt.ProcessSendMessageResponse(new MessageQueue(mq0),
                sendMessageInvocation);
            transaction.TryAddReceipt(publishingMessage, sendReceipt.First());
            
            var mockClientManager = new Mock<IClientManager>();
            producer.SetClientManager(mockClientManager.Object);
            var endTransactionMetadata = producer.Sign();
            var endTransactionResponse = new Proto.EndTransactionResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                }
            };
            var endTransactionInvocation = new RpcInvocation<Proto.EndTransactionRequest, Proto.EndTransactionResponse>(null,
                endTransactionResponse, endTransactionMetadata);
            mockClientManager.Setup(cm => cm.EndTransaction(It.IsAny<Endpoints>(),
                It.IsAny<Proto.EndTransactionRequest>(), It.IsAny<TimeSpan>())).Returns(Task.FromResult(endTransactionInvocation));
            
            producer.State = State.Running;
            await transaction.Commit();
        }
        
        [TestMethod]
        public async Task TestRollback()
        {
            var producer = CreateTestClient();
            var transaction = new Transaction(producer);
            var bytes = Encoding.UTF8.GetBytes("fakeBytes");
            const string tag = "fakeTag";
            var message = new Message.Builder()
                .SetTopic("fakeTopic")
                .SetBody(bytes)
                .SetTag(tag)
                .SetKeys("fakeMsgKey")
                .Build();
            var publishingMessage = transaction.TryAddMessage(message);
            var mq0 = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = "127.0.0.1",
                                Port = 8080
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "foo-bar-namespace",
                    Name = "TestTopic",
                }
            };
            var metadata = producer.Sign();
            var sendResultEntry = new Proto.SendResultEntry
            {
                MessageId = "fakeMsgId",
                TransactionId = "fakeTxId",
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Offset = 1
            };
            var sendMessageResponse = new Proto.SendMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Entries = { sendResultEntry }
            };
            var sendMessageInvocation = new RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>(null,
                sendMessageResponse, metadata);
            var sendReceipt = SendReceipt.ProcessSendMessageResponse(new MessageQueue(mq0),
                sendMessageInvocation);
            transaction.TryAddReceipt(publishingMessage, sendReceipt.First());
            
            var mockClientManager = new Mock<IClientManager>();
            producer.SetClientManager(mockClientManager.Object);
            var endTransactionMetadata = producer.Sign();
            var endTransactionResponse = new Proto.EndTransactionResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                }
            };
            var endTransactionInvocation = new RpcInvocation<Proto.EndTransactionRequest, Proto.EndTransactionResponse>(null,
                endTransactionResponse, endTransactionMetadata);
            mockClientManager.Setup(cm => cm.EndTransaction(It.IsAny<Endpoints>(),
                It.IsAny<Proto.EndTransactionRequest>(), It.IsAny<TimeSpan>())).Returns(Task.FromResult(endTransactionInvocation));
            
            producer.State = State.Running;
            await transaction.Rollback();
        }

        private Producer CreateTestClient()
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build();
            return new Producer(clientConfig, new ConcurrentDictionary<string, bool>(),
                1, null);
        }
    }
}