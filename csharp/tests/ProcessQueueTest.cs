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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class ProcessQueueTest
    {
        [TestMethod]
        public void TestExpired()
        {
            var pushConsumer = CreatePushConsumer("testTopic", 8, 1024,
                4);
            pushConsumer.State = State.Running;
            var processQueue = CreateProcessQueue(pushConsumer);
            Assert.IsFalse(processQueue.Expired());
        }

        [TestMethod]
        public async Task TestReceiveMessageImmediately()
        {
            var pushConsumer = CreatePushConsumer("testTopic", 8, 1024,
                4);
            pushConsumer.State = State.Running;
            var processQueue = CreateProcessQueue(pushConsumer);
            var mockClientManager = new Mock<IClientManager>();
            pushConsumer.SetClientManager(mockClientManager.Object);
            
            var digest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "00000000" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = "127.0.0.1",
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic"
                },
                Body = body
            };
            var receiveMessageResponse0 = new Proto.ReceiveMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                }
            };
            var receiveMessageResponse1 = new Proto.ReceiveMessageResponse
            {
                Message = message
            };
            var metadata = pushConsumer.Sign();
            var receiveMessageResponseList = new List<Proto.ReceiveMessageResponse>
                { receiveMessageResponse0, receiveMessageResponse1 };
            var receiveMessageInvocation =
                new RpcInvocation<Proto.ReceiveMessageRequest, List<Proto.ReceiveMessageResponse>>(null,
                    receiveMessageResponseList, metadata);
            mockClientManager.Setup(cm => cm.ReceiveMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.ReceiveMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(receiveMessageInvocation));

            var receivedMessageCount = 1;
            var mq = new Proto.MessageQueue
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
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                },
                AcceptMessageTypes = { Proto.MessageType.Normal }
            };
            await Task.WhenAny(processQueue.FetchMessageImmediately(), Task.Delay(3000));
            Assert.AreEqual(processQueue.GetCachedMessageCount(), receivedMessageCount);
        }

        [TestMethod]
        public async Task TestEraseMessageWithConsumeOk()
        {
            var pushConsumer = CreatePushConsumer("testTopic", 8, 1024,
                4);
            pushConsumer.State = State.Running;
            var messageView = MessageView.FromProtobuf(CreateMessage(), new MessageQueue(new Proto.MessageQueue
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
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                }
            }));
            var messageViewList = new List<MessageView> { messageView };
            var mockClientManager = new Mock<IClientManager>();
            pushConsumer.SetClientManager(mockClientManager.Object);
            var metadata = pushConsumer.Sign();
            var ackMessageResponse = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                }
            };
            var ackMessageInvocation = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation));
            var processQueue = CreateProcessQueue(pushConsumer);
            processQueue.CacheMessages(messageViewList);
            await processQueue.EraseMessage(messageView, ConsumeResult.SUCCESS);
            mockClientManager.Verify(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()), Times.Once);
        }

        [TestMethod]
        public async Task TestEraseMessageWithFailure()
        {
            var pushConsumer = CreatePushConsumer("testTopic", 8, 1024,
                4);
            pushConsumer.State = State.Running;
            var messageView = MessageView.FromProtobuf(CreateMessage(), new MessageQueue(new Proto.MessageQueue
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
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                }
            }));
            var messageViewList = new List<MessageView> { messageView };
            var mockClientManager = new Mock<IClientManager>();
            pushConsumer.SetClientManager(mockClientManager.Object);
            var metadata = pushConsumer.Sign();
            var ackMessageResponse = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.InternalServerError
                }
            };
            var ackMessageInvocation = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation));
            var processQueue = CreateProcessQueue(pushConsumer);
            processQueue.CacheMessages(messageViewList);
            var ackTimes = 3;
            await Task.WhenAny(processQueue.EraseMessage(messageView, ConsumeResult.SUCCESS),
                Task.Delay(ProcessQueue.AckMessageFailureBackoffDelay.Multiply(2)));
            mockClientManager.Verify(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()), Times.Exactly(ackTimes));
        }
        
        private static ProcessQueue CreateProcessQueue(PushConsumer pushConsumer)
        {
            var mq = new Proto.MessageQueue
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
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                },
                AcceptMessageTypes = { Proto.MessageType.Normal }
            };
            var processQueue = new ProcessQueue(pushConsumer, new MessageQueue(mq),
                pushConsumer.GetSubscriptionExpressions()["testTopic"], new CancellationTokenSource(),
                new CancellationTokenSource(), new CancellationTokenSource(),
                new CancellationTokenSource());
            return processQueue;
        }
        
        private Proto.Message CreateMessage()
        {
            var digest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "9EF61F95" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = "127.0.0.1",
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = "testTopic" },
                Body = body
            };
            return message;
        }

        private PushConsumer CreatePushConsumer(string topic, int maxCacheMessageCount, int maxCacheMessageSizeInBytes,
            int consumptionThreadCount)
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:8080")
                .Build();
            var subscription = new Dictionary<string, FilterExpression>
                { { topic, new FilterExpression("*") } };
            return new PushConsumer(clientConfig, "testGroup",
                new ConcurrentDictionary<string, FilterExpression>(subscription), new TestMessageListener(),
                maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount);
        }
        
        private class TestMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                return ConsumeResult.SUCCESS;
            }
        }
    }
}