// Gumby is ready to go
Gumby.ready(function() {
	console.log('Gumby is ready to go...', Gumby.debug());

	// placeholder polyfil
	if(Gumby.isOldie || Gumby.$dom.find('html').hasClass('ie9')) {
		$('input, textarea').placeholder();
	}
});

// Oldie document loaded
Gumby.oldie(function() {
	console.log("This is an oldie browser...");
});

// Touch devices loaded
Gumby.touch(function() {
	console.log("This is a touch enabled device...");
});

// Document ready
$(function() {
   //$(".strip .row .home").toggle();
    $(".strip .row .myvolumes").toggle();
    $(".strip .row .storage").toggle();
    $(".strip .row .datasets").toggle();

    $(".tabs").on('gumby.onChange', function(e, index) {

        if($(".strip .row .home").hasClass('old-active')) {
            $(".strip .row .home").toggle().removeClass('old-active');
        }
        if($(".strip .row .myvolumes").hasClass('old-active')) {
            $(".strip .row .myvolumes").toggle().removeClass('old-active');
        }
        if($(".strip .row .storage").hasClass('old-active')) {
            $(".strip .row .storage").toggle().removeClass('old-active');
        }
        if($(".strip .row .datasets").hasClass('old-active')) {
            $(".strip .row .datasets").toggle().removeClass('old-active');
        }

        if($(".tab-nav li.home").hasClass('active')) {
            $(".strip .row .home").toggle().addClass('old-active');
        }
        if($(".tab-nav li.myvolumes").hasClass('active')) {
            $(".strip .row .myvolumes").toggle().addClass('old-active');
        }
        if($(".tab-nav li.storage").hasClass('active')) {
            $(".strip .row .storage").toggle().addClass('old-active');
        }
        if($(".tab-nav li.datasets").hasClass('active')) {
            $(".strip .row .datasets").toggle().addClass('old-active');
        }

    })

});

