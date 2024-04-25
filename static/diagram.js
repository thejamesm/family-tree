function scrollToSubject() {
    let iframe = document.querySelector('iframe').contentWindow;
    let svg = iframe.document.querySelector('svg');
    let subject = svg.querySelector('#subject');
    let pixelHeight = svg.clientHeight;
    let unitHeight = svg.getBBox().height;
    let scaleFactor = pixelHeight / unitHeight;
    let subjectPixelX = scaleFactor * subject.getBBox().x;
    let subjectPixelY = scaleFactor * (subject.getBBox().y + unitHeight);
    iframe.scrollTo(subjectPixelX, subjectPixelY);
}

window.onload = scrollToSubject;