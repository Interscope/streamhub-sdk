define([
    'streamhub-sdk/jquery',
    'streamhub-sdk/views/list-view',
    'streamhub-sdk/content/content-view-factory',
    'streamhub-sdk/modal/views/attachment-gallery-modal',
    'inherits',
    'streamhub-sdk/debug',
    'stream/writable',
    'streamhub-sdk/content/views/content-view',
    'streamhub-sdk/views/streams/more',
    'streamhub-sdk/views/show-more-button',
    'streamhub-sdk/views/streams/pond'],
function($, ListView, ContentViewFactory, AttachmentGalleryModal, inherits,
debug, Writable, ContentView, More, ShowMoreButton, Pond) {
    'use strict';

    var log = debug('streamhub-sdk/content/views/content-list-view');

    /**
     * A simple View that displays Content in a list (`<ul>` by default).
     *
     * @param opts {Object} A set of options to config the view with
     * @param opts.el {HTMLElement} The element in which to render the streamed content
     * @exports streamhub-sdk/views/list-view
     * @constructor
     */
    var ContentListView = function(opts) {
        opts = opts || {};

        switch (opts.modal) {
            case true:
            case undefined:
                this.modal = new AttachmentGalleryModal();
                break;
            default:
                this.modal = opts.modal;
        }

        ListView.call(this, opts);

        this._stash = opts.stash || this.more;
        this._maxVisibleItems = opts.maxVisibleItems || 50;
        this._bound = true;
        this.feature = opts.feature || this._createFeatureStream(opts);//TODO (joao) Generalize
        
        this._pipeFeature();//TODO (joao) Generalize

        this.contentViewFactory = opts.contentViewFactory || new ContentViewFactory();
    };

    inherits(ContentListView, ListView);

    ContentListView.prototype.insertingClassName = 'hub-wall-is-inserting';
    ContentListView.prototype.hiddenClassName = 'hub-content-container-hidden';
    ContentListView.prototype.contentContainerClassName = 'hub-content-container';

    /**
     * Class property to add to ListView instances' .el
     */
    ContentListView.prototype.elClass += ' streamhub-content-list-view';

    /**
     * Set the element that this ContentListView renders in
     * @param element {HTMLElement} The element to render the ContentListView in
     */
    ContentListView.prototype.setElement = function (element) {
        ListView.prototype.setElement.apply(this, arguments);

        this.$el.on('removeContentView.hub', $.proxy(function(e, data) {
            return ListView.prototype.remove.call(this, data.contentView);
        }, this));
        this.$el.on('focusContent.hub', $.proxy(function(e, context) {
            var contentView = this.getContentView(context.content);
            if (! this.modal) {
                if (contentView &&
                    contentView.attachmentsView &&
                    typeof contentView.attachmentsView.focus === 'function') {
                    contentView.attachmentsView.focus(context.attachmentToFocus);
                }
                return;
            }
            this.modal.show(context.content, { attachment: context.attachmentToFocus });
        }, this));
    };
    
    /**
     * TODO (joao) Generalize
     * Creates and returns a stream, designed to be used a featured content
     * @param [opts] {Object}
     * @returns {Readable}
     */
    ContentListView.prototype._createFeatureStream = function (opts) {
        opts = opts || {};
        return new Pond({
            highWaterMark: 0,
            goal: 0
        });
    };
    
    /**
     * TODO (joao) Generalize
     */
    ContentListView.prototype._pipeFeature = function () {
        var self = this;
        this.feature.on('readable', function () {
            var content;
            while (content = self.feature.read()) {
                self.add(content, 0);
            }
        });
    };

    /**
     * Comparator function to determine ordering of ContentViews.
     * ContentView elements indexes in this.el will be ordered by this
     * By default, order on contentView.content.createdAt or contentView.createdAt
     *     in descending order (new first)
     * @private
     * @param a {ContentView}
     * @param b {ContentView}
     * @returns {Number} < 0 if a before b, 0 if same ordering, > 0 if b before a
     */
    ContentListView.prototype.comparator = function (a, b) {
        var aDate = a.content.createdAt || a.createdAt,
            bDate = b.content.createdAt || b.createdAt;
        return bDate - aDate;
    };

    ContentListView.prototype.bounded = function (bound) {
        this._bound = bound;
    };

    /**
     * Add a piece of Content to the ContentListView
     *     .createContentView(content)
     *     add newContentView to this.contentViews[]
     *     render the newContentView
     *     insert the newContentView into this.el according to this.comparator
     * @param content {Content} A Content model to add to the ContentListView
     * @param [index] {number} location for the new view
     * @returns the newly created ContentView
     */
    ContentListView.prototype.add = function(content, index) {
        var contentView = content.el ? content : this.getContentView(content); //duck type for ContentView

        log("add", content);

        if (contentView) {
            return ListView.prototype.add.call(this, contentView, index);
        }

        contentView = this.createContentView(content);

        if (this._bound && ! this._hasVisibleVacancy()) {
            var viewToRemove = this.views[this.views.length-1];

            // Ensure .more won't let more through right away,
            // we already have more than we want.
            this.more.setGoal(0);
            // Unshift content to more stream
            this.saveForLater(viewToRemove.content);

            // Remove non visible view
            this.remove(viewToRemove);
        }

        var retVal = ListView.prototype.add.call(this, contentView, index);
        //TODO (joao) These things can't live here
        this.feature._count++;
        if (this.feature._count == this.feature._interval) {
            this.feature._count = -1;
            this.feature.setGoal(1);
        };
        return retVal;
    };

    ContentListView.prototype._insert = function (contentView) {
        var newContentViewIndex,
            $previousEl,
            $wrappedEl;

        newContentViewIndex = this.views.indexOf(contentView);

        var $containerEl = $('<div class="'+this.contentContainerClassName+' '+this.insertingClassName+'"></div>');
        contentView.$el.wrap($containerEl);
        $wrappedEl = contentView.$el.parent();

        if (newContentViewIndex === 0) {
            // Beginning!
            $wrappedEl.prependTo(this.$listEl);
            $wrappedEl.css('margin-top', (-1*$wrappedEl.height())+'px');

            // Wait for the element to be rendered, before removing class which 
            // transitions the margin-top from -100% to 0
            setTimeout($.proxy(function () {
                $wrappedEl.removeClass(this.insertingClassName).css('margin-top', '');
            }, this), 0.1);
        } else {
            // Find it's previous view and insert new view after
            $previousEl = this.views[newContentViewIndex - 1].$el;
            $wrappedEl.removeClass(this.insertingClassName).addClass(this.hiddenClassName);
            $wrappedEl.insertAfter($previousEl.parent('.'+this.contentContainerClassName));

            // Wait for the element to be rendered, before remvoing class which
            // transitions the opacity from 0 to 1
            setTimeout($.proxy(function () {
                $wrappedEl.removeClass(this.hiddenClassName);
            }, this), 0.1);
        }
    };

    /**
     * Remove a view from the DOM. Called by .remove();
     * @private
     * @param view {View} The View to remove from the DOM
     */
    ContentListView.prototype._extract = function (view) {
        view.$el.parent().remove();
    };

    /**
     * Checks if it is still possible to add a content item
     * into the visible list
     * @returns {Boolean} Whether a content item can be displayed
     */
    ContentListView.prototype._hasVisibleVacancy = function () {
        if (this.views.length > this._maxVisibleItems) {
            return false;
        }
        return true;
    };

    /**
     * Save formerly visible content to be redisplayed later
     * @param content {Content} A content item to be redisplayed later
     */
    ContentListView.prototype.saveForLater = function (content) {
        this._stash.stack(content);
    };

    /**
     * Remove a piece of Content from this ContentListView
     * @param content {Content|ContentView} The ContentView or Content to be removed
     * @returns {boolean} true if Content was removed, else false
     */
    ContentListView.prototype.remove = function (content) {
        var contentView = content.el ? content : this.getContentView(content); //duck type for ContentView
        contentView.remove();
    };

    ContentListView.prototype.showMore = function (numToShow) {
        this._bound = false;
        ListView.prototype.showMore.call(this, numToShow);
    };

    /**
     * Given a new Content instance, return an existing contentView that
     * should be used to update the content (based on identity or content.id).
     * @param newContent {Content} The piece of content to find the view for.
     * @returns {ContentView | null} The contentView for the content, or null.
     */
    ContentListView.prototype.getContentView = function (newContent) {
        for (var i=0; i < this.views.length; i++) {
            var contentView = this.views[i];
            if ((newContent === contentView.content) || (newContent.id && contentView.content.id === newContent.id)) {
                return contentView;
            }
        }
        return null;
    };

    /**
     * Creates a content view from the given piece of content, by looking in this view's
     * content registry for the supplied content type.
     * @param content {Content} A content object to create the corresponding view for.
     * @returns {ContentView} A new content view object for the given piece of content.
     */
    ContentListView.prototype.createContentView = function (content) {
        var view = this.contentViewFactory.createContentView(content);

        if (view && typeof view.render === 'function') {
            view.render();
        }

        return view;
    };

    ContentListView.prototype.destroy = function () {
        ListView.prototype.destroy.call(this);
        this.contentViewFactory = null;
    };

    return ContentListView;
});
