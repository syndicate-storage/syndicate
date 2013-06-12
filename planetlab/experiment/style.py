#!/usr/bin/python

STYLESHEET = """
<style type="text/css">html {
  color: rgb(0, 0, 0);
  background-color: rgb(255, 255, 255);
  background-image: none;
  background-repeat: repeat;
  background-attachment: scroll;
  background-position: 0% 0%;
}
body, div, dl, dt, dd, ul, ol, li, h1, h2, h3, h4, h5, h6, pre, code, form, fieldset, legend, input, textarea, p, blockquote, th, td {
  margin-top: 0pt;
  margin-right: 0pt;
  margin-bottom: 0pt;
  margin-left: 0pt;
  padding-top: 0pt;
  padding-right: 0pt;
  padding-bottom: 0pt;
  padding-left: 0pt;
}
table {
  border-collapse: collapse;
  border-spacing: 0pt;
  border-style: hidden hidden;
}
fieldset, img {
  border-top-width: 0pt;
  border-right-width: 0pt;
  border-bottom-width: 0pt;
  border-left-width: 0pt;
  border-top-style: none;
  border-right-style: none;
  border-bottom-style: none;
  border-left-style: none;
}
address, caption, cite, code, dfn, em, strong, th, var {
  font-style: normal;
  font-weight: normal;
}
li {
  list-style-type: none;
  list-style-position: outside;
  list-style-image: none;
}
caption, th {
  text-align: left;
}
h1, h2, h3, h4, h5, h6 {
  font-size: 100%;
  font-weight: normal;
}
q:before, q:after {
  content: "";
}
abbr, acronym {
  border-top-width: 0pt;
  border-right-width: 0pt;
  border-bottom-width: 0pt;
  border-left-width: 0pt;
  border-top-style: none;
  border-right-style: none;
  border-bottom-style: none;
  border-left-style: none;
  font-variant: normal;
}
sup {
  vertical-align: text-top;
}
sub {
  vertical-align: text-bottom;
}
input, textarea, select {
}
input, textarea, select {
}
legend {
  color: rgb(0, 0, 0);
}
del, ins {
  text-decoration: none;
}
body {
  font-style: normal;
  font-variant: normal;
  font-weight: normal;
  font-size: 13px;
  line-height: 1.231;
  font-family: arial, helvetica, clean, sans-serif;
}
select, input, button, textarea {
}
table {
}
pre, code, kbd, samp, tt {
  font-family: monospace;
  line-height: 100%;
}
body {
  text-align: center;
}
#ft {
  clear: both;
}
#doc, #doc2, #doc3, #doc4, .yui-t1, .yui-t2, .yui-t3, .yui-t4, .yui-t5, .yui-t6, .yui-t7 {
  margin-top: auto;
  margin-right: auto;
  margin-bottom: auto;
  margin-left: auto;
  text-align: left;
  width: 57.69em;
  min-width: 750px;
}
#doc2 {
  width: 73.076em;
}
#doc3 {
  margin-top: auto;
  margin-right: 10px;
  margin-bottom: auto;
  margin-left: 10px;
  width: auto;
  color: rgb(252, 252, 252);
  background-color: rgb(54, 54, 54);
}
#doc4 {
  width: 74.923em;
}
.yui-b {
  position: relative;
}
.yui-b {
}
#yui-main .yui-b {
  position: static;
}
#yui-main, .yui-g .yui-u .yui-g {
  width: 100%;
}
.yui-t1 #yui-main, .yui-t2 #yui-main, .yui-t3 #yui-main {
  float: right;
  margin-left: -25em;
}
.yui-t4 #yui-main, .yui-t5 #yui-main, .yui-t6 #yui-main {
  float: left;
  margin-right: -25em;
}
.yui-t1 .yui-b {
  float: left;
  width: 12.3077em;
}
.yui-t1 #yui-main .yui-b {
  margin-left: 13.3077em;
}
.yui-t2 .yui-b {
  float: left;
  width: 13.8461em;
}
.yui-t2 #yui-main .yui-b {
  margin-left: 14.8461em;
}
.yui-t3 .yui-b {
  float: left;
  width: 23.0769em;
}
.yui-t3 #yui-main .yui-b {
  margin-left: 24.0769em;
}
.yui-t4 .yui-b {
  float: right;
  width: 13.8456em;
}
.yui-t4 #yui-main .yui-b {
  margin-right: 14.8456em;
}
.yui-t5 .yui-b {
  float: right;
  width: 18.4615em;
}
.yui-t5 #yui-main .yui-b {
  margin-right: 19.4615em;
}
.yui-t6 .yui-b {
  float: right;
  width: 23.0769em;
}
.yui-t6 #yui-main .yui-b {
  margin-right: 24.0769em;
}
.yui-t7 #yui-main .yui-b {
  display: block;
  margin-top: 0pt;
  margin-right: 0pt;
  margin-bottom: 1em;
  margin-left: 0pt;
}
#yui-main .yui-b {
  float: none;
  width: auto;
}
.yui-gb .yui-u, .yui-g .yui-gb .yui-u, .yui-gb .yui-g, .yui-gb .yui-gb, .yui-gb .yui-gc, .yui-gb .yui-gd, .yui-gb .yui-ge, .yui-gb .yui-gf, .yui-gc .yui-u, .yui-gc .yui-g, .yui-gd .yui-u {
  float: left;
}
.yui-g .yui-u, .yui-g .yui-g, .yui-g .yui-gb, .yui-g .yui-gc, .yui-g .yui-gd, .yui-g .yui-ge, .yui-g .yui-gf, .yui-gc .yui-u, .yui-gd .yui-g, .yui-g .yui-gc .yui-u, .yui-ge .yui-u, .yui-ge .yui-g, .yui-gf .yui-g, .yui-gf .yui-u {
  float: right;
}
.yui-g div.first, .yui-gb div.first, .yui-gc div.first, .yui-gd div.first, .yui-ge div.first, .yui-gf div.first, .yui-g .yui-gc div.first, .yui-g .yui-ge div.first, .yui-gc div.first div.first {
  float: left;
}
.yui-g .yui-u, .yui-g .yui-g, .yui-g .yui-gb, .yui-g .yui-gc, .yui-g .yui-gd, .yui-g .yui-ge, .yui-g .yui-gf {
  width: 49.1%;
}
.yui-gb .yui-u, .yui-g .yui-gb .yui-u, .yui-gb .yui-g, .yui-gb .yui-gb, .yui-gb .yui-gc, .yui-gb .yui-gd, .yui-gb .yui-ge, .yui-gb .yui-gf, .yui-gc .yui-u, .yui-gc .yui-g, .yui-gd .yui-u {
  width: 32%;
  margin-left: 1.99%;
}
.yui-gb .yui-u {
}
.yui-gc div.first, .yui-gd .yui-u {
  width: 66%;
}
.yui-gd div.first {
  width: 32%;
}
.yui-ge div.first, .yui-gf .yui-u {
  width: 74.2%;
}
.yui-ge .yui-u, .yui-gf div.first {
  width: 24%;
}
.yui-g .yui-gb div.first, .yui-gb div.first, .yui-gc div.first, .yui-gd div.first {
  margin-left: 0pt;
}
.yui-g .yui-g .yui-u, .yui-gb .yui-g .yui-u, .yui-gc .yui-g .yui-u, .yui-gd .yui-g .yui-u, .yui-ge .yui-g .yui-u, .yui-gf .yui-g .yui-u {
  width: 49%;
}
.yui-g .yui-g .yui-u {
  width: 48.1%;
}
.yui-g .yui-gb div.first, .yui-gb .yui-gb div.first {
}
.yui-g .yui-gc div.first, .yui-gd .yui-g {
  width: 66%;
}
.yui-gb .yui-g div.first {
}
.yui-gb .yui-gc div.first, .yui-gb .yui-gd div.first {
}
.yui-gb .yui-gb .yui-u, .yui-gb .yui-gc .yui-u {
}
.yui-g .yui-gb .yui-u {
}
.yui-gb .yui-gd .yui-u {
}
.yui-gb .yui-gd div.first {
}
.yui-g .yui-gc .yui-u, .yui-gb .yui-gc .yui-u {
  width: 32%;
  margin-right: 0pt;
}
.yui-gb .yui-gc div.first {
  width: 66%;
}
.yui-gb .yui-ge .yui-u, .yui-gb .yui-gf .yui-u {
  margin-top: 0pt;
  margin-right: 0pt;
  margin-bottom: 0pt;
  margin-left: 0pt;
}
.yui-gb .yui-gb .yui-u {
}
.yui-gb .yui-g div.first, .yui-gb .yui-gb div.first {
}
.yui-gc .yui-g .yui-u, .yui-gd .yui-g .yui-u {
}
.yui-gb .yui-gd div.first {
  width: 32%;
}
.yui-g .yui-gd div.first {
}
.yui-ge .yui-g {
  width: 24%;
}
.yui-gf .yui-g {
  width: 74.2%;
}
.yui-gb .yui-ge div.yui-u, .yui-gb .yui-gf div.yui-u {
  float: right;
}
.yui-gb .yui-ge div.first, .yui-gb .yui-gf div.first {
  float: left;
}
.yui-gb .yui-ge .yui-u, .yui-gb .yui-gf div.first {
}
.yui-gb .yui-ge div.first, .yui-gb .yui-gf .yui-u {
}
.yui-ge div.first .yui-gd .yui-u {
  width: 65%;
}
.yui-ge div.first .yui-gd div.first {
  width: 32%;
}
#bd:after, .yui-g:after, .yui-gb:after, .yui-gc:after, .yui-gd:after, .yui-ge:after, .yui-gf:after {
  content: ".";
  display: block;
  height: 0pt;
  clear: both;
  visibility: hidden;
}
#bd, .yui-g, .yui-gb, .yui-gc, .yui-gd, .yui-ge, .yui-gf {
}
</style><style type="text/css">h1 {
  font-size: 138.5%;
}
h2 {
  font-size: 123.1%;
}
h3 {
  font-size: 108%;
}
h1, h2, h3 {
  margin-top: 1em;
  margin-right: 0pt;
  margin-bottom: 1em;
  margin-left: 0pt;
}
h1, h2, h3, h4, h5, h6, strong {
  font-weight: bold;
}
abbr, acronym {
  border-bottom-width: 1px;
  border-bottom-style: dotted;
  border-bottom-color: rgb(0, 0, 0);
  cursor: help;
}
em {
  font-style: italic;
}
blockquote, ul, ol, dl {
  margin-top: 1em;
  margin-right: 1em;
  margin-bottom: 1em;
  margin-left: 1em;
}
ol, ul, dl {
  margin-left: 2em;
}
ol li {
  list-style-type: decimal;
  list-style-position: outside;
  list-style-image: none;
}
ul li {
  list-style-type: disc;
  list-style-position: outside;
  list-style-image: none;
}
dl dd {
  margin-left: 1em;
}
th, td {
  border-top-width: 1px;
  border-right-width: 1px;
  border-bottom-width: 1px;
  border-left-width: 1px;
  border-top-style: hidden;
  border-right-style: hidden;
  border-bottom-style: hidden;
  border-left-style: solid;
  border-top-color: rgb(0, 0, 0);
  border-right-color: rgb(0, 0, 0);
  border-bottom-color: rgb(0, 0, 0);
  border-left-color: rgb(0, 0, 0);
  padding-top: 0.5em;
  padding-right: 0.5em;
  padding-bottom: 0.5em;
  padding-left: 0.5em;
}
th {
  font-weight: bold;
  text-align: center;
}
caption {
  margin-bottom: 0.5em;
  text-align: center;
}
p, fieldset, table, pre {
  margin-bottom: 1em;
}
input[type="text"], input[type="password"], textarea {
  width: 12.25em;
}
a:link {color: #ff8000; text-decoration: none; }
a:active {color: #ffffff; text-decoration: none; }
a:visited {color: #000000; text-decoration: none; }
a:hover {color: #ff8000; text-decoration: underline; }

#name-text {
  font-size: 0%;
}
#info_text {
  font-size: large;
}
#info_name_header {
  font-size: xx-large;
}
#header1 {
}
</style>
"""

