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
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class SimpleConsumerTest
    {
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestReceiveWithoutStart()
        {
            var consumer = CreateSimpleConsumer();
            await consumer.Receive(16, TimeSpan.FromSeconds(15));
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestAckWithoutStart()
        {
            var consumer = CreateSimpleConsumer();
            var messageView = MessageView.FromProtobuf(CreateMessage());
            await consumer.Ack(messageView);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestSubscribeWithoutStart()
        {
            var consumer = CreateSimpleConsumer();
            await consumer.Subscribe("testTopic", new FilterExpression("*"));
        }
        
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestUnsubscribeWithoutStart()
        {
            var consumer = CreateSimpleConsumer();
            consumer.Unsubscribe("testTopic");
        }
        
        [TestMethod]
        [ExpectedException(typeof(InternalErrorException))]
        public async Task TestReceiveWithZeroMaxMessageNum()
        {
            var consumer = CreateSimpleConsumer();
            consumer.State = State.Running;
            await consumer.Receive(0, TimeSpan.FromSeconds(15));
        }
        
        [TestMethod]
        public async Task TestAck()
        {
            var consumer = CreateSimpleConsumer();
            consumer.State = State.Running;
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
            var mockClientManager = new Mock<IClientManager>();
            consumer.SetClientManager(mockClientManager.Object);
            
            var metadata = consumer.Sign();
            var ackMessageResponse0 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                }
            };
            var ackMessageInvocation0 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse0, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation0));
            await consumer.Ack(messageView);
            
            
            var ackMessageResponse1 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.BadRequest
                }
            };
            var ackMessageInvocation1 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse1, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation1));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var ackMessageResponse2 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.IllegalTopic
                }
            };
            var ackMessageInvocation2 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse2, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation2));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var ackMessageResponse3 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.IllegalConsumerGroup
                }
            };
            var ackMessageInvocation3 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse3, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation3));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var ackMessageResponse4 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.InvalidReceiptHandle
                }
            };
            var ackMessageInvocation4 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse4, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation4));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var ackMessageResponse5 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.ClientIdRequired
                }
            };
            var ackMessageInvocation5 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse5, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation5));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var ackMessageResponse6 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Unauthorized
                }
            };
            var ackMessageInvocation6 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse6, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation6));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(UnauthorizedException));
            }
            
            
            var ackMessageResponse7 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Forbidden
                }
            };
            var ackMessageInvocation7 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse7, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation7));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(ForbiddenException));
            }
            
            
            var ackMessageResponse8 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.NotFound
                }
            };
            var ackMessageInvocation8 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse8, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation8));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(NotFoundException));
            }
            
            
            var ackMessageResponse9 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.TopicNotFound
                }
            };
            var ackMessageInvocation9 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse9, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation9));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(NotFoundException));
            }
            
            
            var ackMessageResponse10 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.TooManyRequests
                }
            };
            var ackMessageInvocation10 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse10, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation10));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(TooManyRequestsException));
            }
            
            
            var ackMessageResponse11 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.InternalError
                }
            };
            var ackMessageInvocation11 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse11, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation11));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(InternalErrorException));
            }
            
            
            var ackMessageResponse12 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.InternalServerError
                }
            };
            var ackMessageInvocation12 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse12, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation12));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(InternalErrorException));
            }
            
            
            var ackMessageResponse13 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.ProxyTimeout
                }
            };
            var ackMessageInvocation13 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse13, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation13));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(ProxyTimeoutException));
            }
            
            
            var ackMessageResponse14 = new Proto.AckMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Unsupported
                }
            };
            var ackMessageInvocation14 = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null,
                ackMessageResponse14, metadata);
            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(ackMessageInvocation14));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(UnsupportedException));
            }
        }
        
        [TestMethod]
        public async Task TestChangeInvisibleDuration()
        {
            var consumer = CreateSimpleConsumer();
            consumer.State = State.Running;
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
            var invisibleDuration = TimeSpan.FromSeconds(3);
            var mockClientManager = new Mock<IClientManager>();
            consumer.SetClientManager(mockClientManager.Object);
            
            var metadata = consumer.Sign();
            var changeInvisibleTimeResponse0 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                }
            };
            var changeInvisibleTimeInvocation0 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse0, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation0));
            await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            
            
            var changeInvisibleTimeResponse1 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.BadRequest
                }
            };
            var changeInvisibleTimeInvocation1 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                changeInvisibleTimeResponse1, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation1));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var changeInvisibleTimeResponse2 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.IllegalTopic
                }
            };
            var changeInvisibleTimeInvocation2 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse2, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation2));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var changeInvisibleTimeResponse3 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.IllegalConsumerGroup
                }
            };
            var changeInvisibleTimeInvocation3 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse3, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation3));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var changeInvisibleTimeResponse4 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.IllegalInvisibleTime
                }
            };
            var changeInvisibleTimeInvocation4 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse4, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation4));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var changeInvisibleTimeResponse5 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.InvalidReceiptHandle
                }
            };
            var changeInvisibleTimeInvocation5 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse5, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation5));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var changeInvisibleTimeResponse6 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.ClientIdRequired
                }
            };
            var changeInvisibleTimeInvocation6 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse6, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation6));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(BadRequestException));
            }
            
            
            var changeInvisibleTimeResponse7 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Unauthorized
                }
            };
            var changeInvisibleTimeInvocation7 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse7, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation7));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(UnauthorizedException));
            }
            
            
            var changeInvisibleTimeResponse8 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.NotFound
                }
            };
            var changeInvisibleTimeInvocation8 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse8, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation8));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(NotFoundException));
            }
            
            
            var changeInvisibleTimeResponse9 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.TopicNotFound
                }
            };
            var changeInvisibleTimeInvocation9 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse9, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation9));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(NotFoundException));
            }
            
            
            var changeInvisibleTimeResponse10 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.TooManyRequests
                }
            };
            var changeInvisibleTimeInvocation10 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse10, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation10));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(TooManyRequestsException));
            }
            
            
            var changeInvisibleTimeResponse11 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.InternalError
                }
            };
            var changeInvisibleTimeInvocation11 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse11, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation11));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(InternalErrorException));
            }
            
            
            var changeInvisibleTimeResponse12 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.InternalServerError
                }
            };
            var changeInvisibleTimeInvocation12 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse12, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation12));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(InternalErrorException));
            }
            
            
            var changeInvisibleTimeResponse13 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.ProxyTimeout
                }
            };
            var changeInvisibleTimeInvocation13 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse13, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation13));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(ProxyTimeoutException));
            }
            
            
            var changeInvisibleTimeResponse14 = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Unsupported
                }
            };
            var changeInvisibleTimeInvocation14 =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    changeInvisibleTimeResponse14, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(changeInvisibleTimeInvocation14));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(UnsupportedException));
            }
        }
        
        private SimpleConsumer CreateSimpleConsumer()
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build();
            var subscription = new Dictionary<string, FilterExpression>
                { { "testTopic", new FilterExpression("*") } };
            var consumer = new SimpleConsumer(clientConfig, "testConsumerGroup",
                TimeSpan.FromSeconds(15), subscription);
            return consumer;
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
    }
}