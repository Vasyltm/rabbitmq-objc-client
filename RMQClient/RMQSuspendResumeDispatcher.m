// This source code is dual-licensed under the Mozilla Public License ("MPL"),
// version 2.0 and the Apache License ("ASL"), version 2.0.
//
// The ASL v2.0:
//
// ---------------------------------------------------------------------------
// Copyright 2017-2022 VMware, Inc. or its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ---------------------------------------------------------------------------
//
// The MPL v2.0:
//
// ---------------------------------------------------------------------------
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
//
// Alternatively, the contents of this file may be used under the terms
// of the Apache Standard license (the "ASL License"), in which case the
// provisions of the ASL License are applicable instead of those
// above. If you wish to allow use of your version of this file only
// under the terms of the ASL License and not to allow others to use
// your version of this file under the MPL, indicate your decision by
// deleting the provisions above and replace them with the notice and
// other provisions required by the ASL License. If you do not delete
// the provisions above, a recipient may use your version of this file
// under either the MPL or the ASL License.
// ---------------------------------------------------------------------------

#import "RMQSuspendResumeDispatcher.h"
#import "RMQErrors.h"
#import "RMQDispatcher.h"

@interface RMQSuspendResumeDispatcher ()
@property (nonatomic,weak, readwrite) id<RMQChannel> channel;
@property (nonatomic,weak, readwrite) id<RMQSender> sender;
@property (nonatomic, readwrite) RMQFramesetValidator *validator;
@property (nonatomic, readwrite) id<RMQLocalSerialQueue> commandQueue;
@property (nonatomic, readwrite) id<RMQLocalSerialQueue> enablementQueue;
@property (nonatomic, readwrite) NSNumber *enableDelay;
@property (nonatomic,weak, readwrite) id<RMQConnectionDelegate> delegate;
@property (nonatomic, readwrite) DispatcherState state;
@property (nonatomic, readwrite) BOOL disabled;
@end

@implementation RMQSuspendResumeDispatcher

- (instancetype)initWithSender:(id<RMQSender>)sender
                  commandQueue:(id<RMQLocalSerialQueue>)commandQueue
               enablementQueue:(id<RMQLocalSerialQueue>)enablementQueue
                   enableDelay:(NSNumber *)enableDelay {
    self = [super init];
    if (self) {
        self.channel = nil;
        self.sender = sender;
        self.validator = [RMQFramesetValidator new];
        self.commandQueue = commandQueue;
        self.enablementQueue = enablementQueue;
        self.enableDelay = enableDelay;
        self.state = DispatcherStateOpen;
        self.disabled = NO;
    }
    return self;
}

- (instancetype)initWithSender:(id<RMQSender>)sender
                  commandQueue:(id<RMQLocalSerialQueue>)commandQueue {
    return [self initWithSender:sender commandQueue:commandQueue enablementQueue:nil enableDelay:@0];
}

- (void)activateWithChannel:(id<RMQChannel>)channel
                   delegate:(id<RMQConnectionDelegate>)delegate {
    self.channel = channel;
    self.delegate = delegate;
    [self.commandQueue resume];
}

- (void)blockingWaitOn:(Class)method {
    __weak id this = self;
    [self.commandQueue blockingEnqueue:^{
        __strong typeof(self) strongThis = this;
        [strongThis processOutgoing:nil executeOrErr:^{
            [strongThis.commandQueue suspend];
        }];
    }];

    [self.commandQueue blockingEnqueue:^{
        __strong typeof(self) strongThis = this;
        RMQFramesetValidationResult *result = [strongThis.validator expect:method];
        if (result.error) {
            [strongThis.delegate channel:strongThis.channel error:result.error];
        }
    }];
}

- (void)sendSyncMethod:(id<RMQMethod>)method
     completionHandler:(void (^)(RMQFrameset *frameset))completionHandler {

    __weak id this = self;

    [self.commandQueue enqueue:^{
        __strong typeof(self) strongThis = this;
        [strongThis processOutgoing:method executeOrErr:^{
            if ([strongThis isChannelClose:method]) {
                [strongThis processUserInitiatedChannelClose];
            }

            RMQFrameset *outgoingFrameset = [[RMQFrameset alloc] initWithChannelNumber:strongThis.channelNumber method:method];
            [strongThis.commandQueue suspend];
            [strongThis.sender sendFrameset:outgoingFrameset];
        }];
    }];

    [self.commandQueue enqueue:^{
        __strong typeof(self) strongThis = this;
        RMQFramesetValidationResult *result = [strongThis.validator expect:method.syncResponse];
        if (strongThis.isOpen && result.error) {
            [strongThis.delegate channel:strongThis.channel error:result.error];
        } else if (strongThis.isOpen || [strongThis isChannelClose:method]) {
            // execute completion handlers when open
            // but special case user-initiated channel.close methods
            completionHandler(result.frameset);
        }
    }];
}

- (void)sendSyncMethod:(id<RMQMethod>)method {
    [self sendSyncMethod:method completionHandler:^(RMQFrameset *frameset) {}];
}

- (void)sendSyncMethodBlocking:(id<RMQMethod>)method {
    __weak id this = self;
    [self.commandQueue blockingEnqueue:^{
        __strong typeof(self) strongThis = this;
        [strongThis processOutgoing:method executeOrErr:^{
            if ([strongThis isChannelClose:method]) {
                [strongThis processUserInitiatedChannelClose];
            }

            RMQFrameset *frameset = [[RMQFrameset alloc] initWithChannelNumber:strongThis.channelNumber method:method];
            [strongThis.commandQueue suspend];
            [strongThis.sender sendFrameset:frameset];
        }];
    }];

    [self.commandQueue blockingEnqueue:^{
        __strong typeof(self) strongThis = this;
        RMQFramesetValidationResult *result = [strongThis.validator expect:method.syncResponse];

        if (strongThis.isOpen && result.error) {
            [strongThis.delegate channel:strongThis.channel error:result.error];
        }
    }];
}

- (void)sendAsyncFrameset:(RMQFrameset *)frameset {
    __weak id this = self;
    [self.commandQueue enqueue:^{
        __strong typeof(self) strongThis = this;
        [strongThis processOutgoing:frameset.method executeOrErr:^{
            [strongThis.sender sendFrameset:frameset];
        }];
    }];
}

- (void)sendAsyncMethod:(id<RMQMethod>)method {
    [self sendAsyncFrameset:[[RMQFrameset alloc] initWithChannelNumber:self.channelNumber method:method]];
}

- (void)enqueue:(RMQOperation)operation {
    [self.commandQueue enqueue:operation];
}

- (void)disable {
    self.disabled = YES;
    [self.commandQueue suspend];
}

- (void)enable {
    __weak id this = self;
    [self.enablementQueue delayedBy:self.enableDelay enqueue:^{
        __strong typeof(self) strongThis = this;
        strongThis.disabled = NO;
        [strongThis.commandQueue resume];
    }];
}

- (BOOL)isOpen {
    return self.state == DispatcherStateOpen;
}

- (BOOL) wasClosedByServer {
    return self.state == DispatcherStateClosedByServer;
}

- (BOOL) wasClosedExplicitly {
    return self.state == DispatcherStateClosedByClient;
}

- (void)handleFrameset:(RMQFrameset *)frameset {
    if (!self.wasClosedByServer && [self isChannelClose:frameset.method]) {
        [self processServerSentChannelClose:(RMQChannelClose *)frameset.method];
    } else if (self.isOpen) {
        [self.validator fulfill:frameset];
    }
    if (!self.disabled) {
        [self.commandQueue resume];
    }
}

# pragma mark - Private

- (void)processOutgoing:(id<RMQMethod>)method
           executeOrErr:(void (^)(void))operation {
    if (self.isOpen) {
        operation();
    } else if (![self isChannelClose:method]) {
        [self sendChannelClosedError];
    }
}

- (void)processUserInitiatedChannelClose {
    self.state = DispatcherStateClosedByClient;
}

- (void)processServerSentChannelClose:(RMQChannelClose *)close {
    self.state = DispatcherStateClosedByServer;
    NSError *error = [NSError errorWithDomain:RMQErrorDomain
                                         code:close.replyCode.integerValue
                                     userInfo:@{NSLocalizedDescriptionKey: close.replyText.stringValue}];
    [self.delegate channel:self.channel error:error];
    [self.sender sendFrameset:[[RMQFrameset alloc] initWithChannelNumber:self.channelNumber
                                                                  method:[RMQChannelCloseOk new]]];
}

- (void)sendChannelClosedError {
    NSError *error = [NSError errorWithDomain:RMQErrorDomain
                                         code:RMQErrorChannelClosed
                                     userInfo:@{NSLocalizedDescriptionKey: @"Cannot use channel after it has been closed."}];
    [self.delegate channel:self.channel error:error];
}

- (BOOL)isChannelClose:(id<RMQMethod>)method {
    return [method isKindOfClass:[RMQChannelClose class]];
}

- (NSNumber *)channelNumber {
    return self.channel.channelNumber;
}

@end
