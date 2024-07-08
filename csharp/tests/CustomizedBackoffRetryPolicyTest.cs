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
using System.Linq;
using Apache.Rocketmq.V2;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class CustomizedBackoffRetryPolicyTest
    {
        [TestMethod]
        public void TestConstructWithValidDurationsAndMaxAttempts()
        {
            // Arrange
            var durations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) };
            var maxAttempts = 3;

            // Act
            var policy = new CustomizedBackoffRetryPolicy(durations, maxAttempts);

            // Assert
            Assert.AreEqual(maxAttempts, policy.GetMaxAttempts());
            CollectionAssert.AreEqual(durations, policy.GetDurations());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestConstructWithEmptyDurations()
        {
            // Arrange & Act
            new CustomizedBackoffRetryPolicy(new List<TimeSpan>(), 3);

            // Assert is handled by ExpectedException
        }

        [TestMethod]
        public void TestGetNextAttemptDelayWithValidAttempts()
        {
            // Arrange
            var durations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5) };
            var policy = new CustomizedBackoffRetryPolicy(durations, 5);

            // Act & Assert
            Assert.AreEqual(TimeSpan.FromSeconds(1), policy.GetNextAttemptDelay(1));
            Assert.AreEqual(TimeSpan.FromSeconds(3), policy.GetNextAttemptDelay(2));
            Assert.AreEqual(TimeSpan.FromSeconds(5), policy.GetNextAttemptDelay(3));
            Assert.AreEqual(TimeSpan.FromSeconds(5), policy.GetNextAttemptDelay(4)); // Should inherit the last duration
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestGetNextAttemptDelayWithInvalidAttempt()
        {
            // Arrange
            var durations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) };
            var policy = new CustomizedBackoffRetryPolicy(durations, 3);

            // Act
            policy.GetNextAttemptDelay(0);

            // Assert is handled by ExpectedException
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestGetNextAttemptDelayWithNegativeAttempt()
        {
            // Arrange
            var durations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) };
            var policy = new CustomizedBackoffRetryPolicy(durations, 3);

            // Act
            policy.GetNextAttemptDelay(-1);

            // Assert is handled by ExpectedException
        }

        [TestMethod]
        public void TestFromProtobufWithValidRetryPolicy()
        {
            // Arrange
            var protoDurations = new List<Duration>
            {
                Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                Duration.FromTimeSpan(TimeSpan.FromSeconds(2))
            };
            var protoRetryPolicy = new RetryPolicy
            {
                MaxAttempts = 3,
                CustomizedBackoff = new CustomizedBackoff { Next = { protoDurations } },
            };

            // Act
            var policy = CustomizedBackoffRetryPolicy.FromProtobuf(protoRetryPolicy);

            // Assert
            Assert.AreEqual(3, policy.GetMaxAttempts());
            Assert.AreEqual(protoDurations.Count, policy.GetDurations().Count);
            foreach (var (expected, actual) in protoDurations.Zip(policy.GetDurations(), Tuple.Create))
            {
                Assert.AreEqual(expected.ToTimeSpan(), actual);
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestFromProtobufWithInvalidRetryPolicy()
        {
            var initialBackoff0 = Duration.FromTimeSpan(TimeSpan.FromSeconds(1));
            var maxBackoff0 = Duration.FromTimeSpan(TimeSpan.FromSeconds(1));
            var backoffMultiplier = 1.0f;
            var maxAttempts = 3;

            var exponentialBackoff = new ExponentialBackoff
            {
                Initial = initialBackoff0,
                Max = maxBackoff0,
                Multiplier = backoffMultiplier
            };

            var retryPolicy = new RetryPolicy
            {
                MaxAttempts = maxAttempts,
                ExponentialBackoff = exponentialBackoff
            };

            CustomizedBackoffRetryPolicy.FromProtobuf(retryPolicy);
        }
        
        [TestMethod]
        public void ToProtobuf_ShouldReturnCorrectProtobuf()
        {
            // Arrange
            var durations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) };
            var maxAttempts = 3;
            var policy = new CustomizedBackoffRetryPolicy(durations, maxAttempts);

            // Act
            var proto = policy.ToProtobuf();

            // Assert
            Assert.AreEqual(maxAttempts, proto.MaxAttempts);
            Assert.AreEqual(durations.Count, proto.CustomizedBackoff.Next.Count);
            for (var i = 0; i < durations.Count; i++)
            {
                Assert.AreEqual(durations[i], proto.CustomizedBackoff.Next[i].ToTimeSpan());
            }
        }

        [TestMethod]
        public void TestInheritBackoffWithValidCustomizedBackoffPolicy()
        {
            // Arrange
            var originalDurations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3) };
            var newDurations = new List<Duration>
            {
                Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                Duration.FromTimeSpan(TimeSpan.FromSeconds(4))
            };
            var backoff = new CustomizedBackoff { Next = { newDurations } };
            var retryPolicy = new RetryPolicy
            {
                MaxAttempts = 5,
                CustomizedBackoff = backoff,
            };
            var policy = new CustomizedBackoffRetryPolicy(originalDurations, 5);

            // Act
            var inheritedPolicy = policy.InheritBackoff(retryPolicy);

            // Assert
            Assert.IsTrue(inheritedPolicy is CustomizedBackoffRetryPolicy);
            var customizedBackoffRetryPolicy = (CustomizedBackoffRetryPolicy) inheritedPolicy;
            Assert.AreEqual(policy.GetMaxAttempts(), inheritedPolicy.GetMaxAttempts());
            var inheritedDurations = customizedBackoffRetryPolicy.GetDurations();
            Assert.AreEqual(newDurations.Count, inheritedDurations.Count);
            for (var i = 0; i < newDurations.Count; i++)
            {
                Assert.AreEqual(newDurations[i].ToTimeSpan(), inheritedDurations[i]);
            }
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestInheritBackoffWithInvalidPolicy()
        {
            var maxAttempt = 3;

            var durations1 = new List<TimeSpan>
            {
                TimeSpan.FromSeconds(3),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(1)
            };

            var retryPolicy0 = new CustomizedBackoffRetryPolicy(durations1, maxAttempt);

            var exponentialBackoff = new ExponentialBackoff();

            var retryPolicy = new RetryPolicy
            {
                ExponentialBackoff = exponentialBackoff,
            };

            retryPolicy0.InheritBackoff(retryPolicy);
        }
    }
}