/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @author qb.wu@outlook.com
 * @date 2017/4/27
 */

package com.xxxxx.xxxxxxx.streams

final class Notifier {
    private[this] var flag = false
    def await {
        this.synchronized {
            while (!flag) this.wait
            // can not stop ssc here because of possible deadlock between
            // this and async listener
            flag = true
        }
    }

    def awake {
        this.synchronized {
            flag = true
            this.notify
        }
    }

    def reset {
        this.synchronized {
            flag = false
        }
    }

    def state = {
        this.synchronized {
            flag
        }
    }
}
