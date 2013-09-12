define([
    'inherits',
    'stream/readable',
    'stream/util',
    'streamhub-sdk/clients/livefyre-bootstrap-client',
    'streamhub-sdk/clients/livefyre-stream-client',
    'streamhub-sdk/content/state-to-content',
    'streamhub-sdk/debug'],
function (inherits, Readable, streamUtil, BootstrapClient, StreamClient,
StateToContent, debug) {


    var log = debug('streamhub-sdk/streams/collection-updater');


    /**
     * A Readable Stream to access streaming updates in a StreamHub Collection
     * @param opts {object}
     * @param opts.network {string} The StreamHub Network of the Collection
     * @param opts.siteId {string} The Site ID of the Collection
     * @param opts.articleId {string} The Article ID of the Collection
     * @param [opts.environment] {string} Which StreamHub Cluster the Collection
     *     resides on (e.g. t402.livefyre.com for UAT)
     * @param [opts.streamClient] {LivefyreStreamClient} A Client object that
     *     can request StreamHub's Stream web service
     * @param [opts.bootstrapClient] {LivefyreBootstrapClient} A Client object
     *     that can request StreamHub's Bootstrap web service
     */
    var CollectionUpdater = function (opts) {
        opts = opts || {};

        this._network = opts.network;
        this._siteId = opts.siteId;
        this._articleId = opts.articleId;
        this._environment = opts.environment;

        this._streamClient = opts.streamClient || StreamClient;
        this._bootstrapClient = opts.bootstrapClient || BootstrapClient;
        this._isInitingFromBootstrap = false;
        this._finishedInitFromBootstrap = false;
        this._contentIdsInHeadDocument = [];

        Readable.call(this, opts);
    }

    inherits(CollectionUpdater, Readable);


    /**
     * @private
     * Called by Readable base class on .read(). Do not call directly.
     * Get content from bootstrap and .push() onto the read buffer
     */
    CollectionUpdater.prototype._read = function () {
        var self = this,
            bootstrapClientOpts,
            stateToPush;
        log('_read', 'Buffer length is ' + this._readableState.buffer.length);

        // The first time this is called, we first need to get Bootstrap init
        // to know what the latest page of data
        if ( ! this._finishedInitFromBootstrap) {
            log('requesting bootstrap init');
            return this._getBootstrapInit(function (err, initResponse) {
                var collectionSettings = initResponse.collectionSettings,
                    latestEvent = collectionSettings.event,
                    collectionId = collectionSettings.collectionId;

                self._latestEvent = latestEvent;
                self._collectionId = collectionId;

                self._stream();
            });
        }
        self._stream();
    };


    /**
     * @private
     * Make the next stream request to get more data since the last seen event
     */
    CollectionUpdater.prototype._stream = function () {
        var self = this,
            streamClient = this._streamClient,
            latestEvent = this._latestEvent,
            streamClientOpts = this._getCollectionOptions();
        // Request stream from the last Event ID we know about
        streamClientOpts.commentId = latestEvent;
        streamClient.getContent(streamClientOpts, function (err, data) {
            if (err) {
                return self.emit('error', err);
            }
            if (data.timeout) {
                // Timed out on the long poll. This just means there
                // was no real-time data. So we should keep streaming
                // on the next event loop tick
                log('long poll timeout, requesting again on next tick');
                return streamUtil.nextTick(function () {
                    self._stream();
                });
            }
            var contents = self._contentsFromStreamData(data);
            // Update _latestEvent so we only get new data
            self._latestEvent = data.maxEventId;

            if (contents.length) {
                self.push.apply(self, contents);
                // _read will get called again when more data is needed
            } else {
                self._stream();
            }
        });
    };


    /**
     * @private
     * Convert a response from the Stream service into Content models
     * @param streamData {object} A response from the Stream service
     * @return {Content[]} An Array of Content models
     */
    CollectionUpdater.prototype._contentsFromStreamData = function (streamData) {
        var states = streamData.states,
            stateToContent = new StateToContent(streamData),
            state,
            content,
            contents = [];

        stateToContent.on('data', function (content) {
            contents.push(content);
        });

        for (var contentId in states) {
            if (states.hasOwnProperty(contentId)) {
                state = states[contentId];
                stateToContent.write(state);
            }
        }

        return contents;
    };


    /**
     * @private
     * Request the Bootstrap init endpoint for the Collection to learn
     * what the latest event is. This gets called the first time Stream
     * base calls _read().
     * @param errback {function} A callback to be passed (err|null, the number
     *     of pages of content in the collection, the headDocument containing
     *     the latest data)
     */
    CollectionUpdater.prototype._getBootstrapInit = function (errback) {
        var self = this,
            collectionOpts;

        if (this._isInitingFromBootstrap) {
            log("_getBootstrapInit was called, but I'm already requesting " +
                "init and haven't gotten a response. This probably shouldn't " +
                "happen.");
            return;
        }
        this._isInitingFromBootstrap = true;

        // Use this._bootstrapClient to request init (init is default when
        // no opts.page is specified)
        collectionOpts = this._getCollectionOptions();
        this._bootstrapClient.getContent(collectionOpts, function (err, data) {
            if (err) {
                log("Error requesting Bootstrap init", err, data);
                self.emit('error', err);
            }

            self._isInitingFromBootstrap = false;
            self._finishedInitFromBootstrap = true;

            errback.call(self, err, data);
        });
    };


    /**
     * @private
     * Get options to pass to this._bootstrapClient methods to specify
     * which Collection we care about
     */
    CollectionUpdater.prototype._getCollectionOptions = function () {
        return {
            environment: this._environment,
            network: this._network,
            siteId: this._siteId,
            articleId: this._articleId,
            collectionId: this._collectionId
        };
    };


    return CollectionUpdater;
});