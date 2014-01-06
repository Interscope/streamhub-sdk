define([
    'inherits',
    'stream/duplex',
    'streamhub-sdk/content/views/content-list-view',
    'streamhub-sdk/debug'],
function (inherits, Duplex, ContentListView, debug) {
    'use strict';


    var log = debug('streamhub-sdk/views/streams/pond');


    /**
     * A Duplex stream (Readable & Writable) that only passes through
     * the number of items it is instructed to.
     * Pond also maintains a LIFO stack such that previously emitted Content can
     * be stashed back into Pond so that it is re-read out later when needed.
     * @constructor
     * @param opts {object}
     * @param [opts.goal=0] {number} The initial amount to let through
     */
    var Pond = function (opts) {
        opts = opts || {};
        this.SCOOPS = opts.scoops || this.SCOOPS;
        this._stack = [];
        this._requestMore = null;
        this._interval = opts.interval || 1;
        this._count = 0;
        this._previousWrite = null;
        this._contentListView = null;
        Duplex.call(this, opts);
    };
    inherits(Pond, Duplex);

    
    /**
     * The default number of scoops when no number is provided.
     * @type {!number} Should be greater than 0.
     */
    Pond.prototype.SCOOPS = 1;
    
    /**
     * Registers a ContentListView or subclass. Listens for added contentViews and
     * pushes at the specified intervals.
     * @param contentListView {ContentListView}
     * @param [opts} {Object}
     */
    Pond.prototype.pipish = function(contentListView, opts) {
        this._contentListView = contentListView;
        this._contentListView.on('added', function (contentView) {
            if (!contentView) {
                return;
            }
            
            if (contentView.content !== this._previousWrite) {
                this._count++;
            }
            
            if (this._count === this._interval) {
                this._count = 0;
                var index = this._contentListView.views.indexOf(contentView) || 0;
                this.setGoal(undefined, index);
            }
        }.bind(this));
    };

    /**
     * Let more items pass through.
     * This sets the goal of the stream to the provided number.
     * @param newGoal {number} The number of items this stream should
     * @param index {number} Index where the last item was inserted
     *     let through before holding again.
     */
    Pond.prototype.setGoal = function (newGoal, index) {
        this._index = index;
        this.scoop(newGoal);
    };
    
    
    /**
     * Lets 1 or the specified number of items pass through.
     * @param [n] {number=}
     */
    Pond.prototype.scoop = function (n) {
        this._scoops = n || this.SCOOPS;
        
        if (this._scoops >= 0) {
            this._fetchAndPush();
        }
    };


    /**
     * Get the number of objects the stream is waiting for to reach its goal
     */
    Pond.prototype.getGoal = function () {
        return this._scoops;
    };


    /**
     * stack Content that should be re-emitted later in last-in-first-out
     * fashion. stacked stuff is read out before written stuff
     * @param obj {Object} An object to stack, that you may want back later
     */
    Pond.prototype.stack = function (obj) {
        this._stack.push(obj);
    };


    /**
     * Required by Duplex subclasses.
     * This ensures that once the goal is reached, no more content
     * passes through.
     * @private
     */
    Pond.prototype._write = function (chunk, doneWriting) {
        var self = this;
        log('_write', chunk);

        // Put on BOTTOM of the stack.
        // written stuff comes after all stacked stuff
        this._stack.unshift(chunk);

        // Save the doneWriting cb for later. We'll call it once this
        // new bottom of the stack is popped, and we need more data
        // from the Writable side of the duplex
        this._requestMore = function () {
            self._requestMore = null;
            doneWriting();
        };

        if (this._scoops >= 1) {
            this._fetchAndPush();
        } else {
            // Emit 'hold' to signify that there is data waiting, if only
            // the goal were increased. This is useful to render a 'show more'
            // button only if there is data in the buffer, and avoids a
            // show more button that, when clicked, does nothing but disappear
            this.emit('hold');
        }
    };


    /**
     * Required by Readable subclasses. Get data from upstream. In this case,
     * either the internal ._stack or the Writable side of the Duplex
     * @private
     */
    Pond.prototype._read = function () {
        if (this._scoops <= 0 && this._stack.length) {
            // You don't get data yet.
            this.emit('hold');
            return;
        }
        this._fetchAndPush();
    };


    /**
     * Fetch data from the internal stack (sync) and push it.
     * Or, if there is nothing in the stack, request more from the Writable
     * side of the duplex, which will eventually call this again.
     * @private
     */
    Pond.prototype._fetchAndPush = function () {
        // If there's data in the stack, pop, push it along, & decrement goal
        if (this._stack.length) {
            // There's stuff in the stack. Push it.
            this._scoops--;
//            this.push(this._stack.pop());
            this._previousWrite = this._stack.pop();
            this._contentListView.add(this._previousWrite, this._index);
        }

        // If there was no data, or we just pushed the last bit,
        // request more if possible
        if (this._stack.length === 0 &&
            typeof this._requestMore === 'function') {
            this._requestMore();
        }
    };


    return Pond;
});
