import re
import pymorphy2
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
import pickle
import logging
import os
from pathlib import Path
from multiprocessing import Pool
import multiprocessing

morph = pymorphy2.MorphAnalyzer()


class Index:
    def __init__(self, db_service, store, index_path):
        self.__db_cursor = db_service.cur
        self.__store = store
        self.__common_dict = {}
        self.__index_path = index_path

    def create_for_first_latter(self):
        p = Pool(4)
        letters = [chr(i) for i in range(ord('а'), ord('я') + 1)]
        letters.extend([chr(i + 48) for i in range(10)])
        letters.append('ё')
        self.__common_dict = {key: dict() for key in letters}
        input = self.__get_documents()
        p.map(pre_process_letter, [(input, ch, self.__common_dict[ch]) for ch in letters])
        return self.__common_dict

    def create(self):
        p = Pool(4)
        input = self.__get_documents()
        data = filter(None.__ne__, p.map(pre_process, input))
        for doc_id, words in data:
            for word in words.keys():
                if word not in self.__common_dict:
                    self.__common_dict[word] = []
                self.__common_dict[word].append((doc_id, words[word]))
        return self.__common_dict

    def __get_documents(self):
        cmd = 'SELECT id, url FROM storage'
        self.__db_cursor.execute(cmd)
        result = self.__db_cursor.fetchall()
        return [(idx, self.__store.url_to_path(url)) for idx, url in result]

    def serialize(self, file_name):
        with open(os.path.join(self.__index_path, file_name), 'wb') as f:
            try:
                pickle.dump(self.__common_dict, f)
            except pickle.PicklingError as e:
                logging.error(str(e))

    def deserialize(self, file_name):
        with open(os.path.join(self.__index_path, file_name), 'rb') as f:
            try:
                self.__common_dict = pickle.load(f)
            except pickle.UnpicklingError as e:
                logging.error(str(e))


def pre_process(tuples):
    idx, full_path = tuples
    path = Path(os.path.join(full_path, 'content.txt'))
    if path.exists():
        raw_data = path.read_text()
        raw_data = BeautifulSoup(raw_data, 'lxml').getText()
        words = re.sub(r'[^А-я0-9ёЁ ]', '', raw_data).split()
        words = [morph.parse(word)[0].normal_form for word in words if word not in stopwords.words('russian')]
        result = {}
        for i, word in enumerate(words):
            if word not in result:
                result[word] = []
            result[word].append(i)
        return idx, result


def pre_process_letter(tuples):
    input, ch, dict_ch = tuples
    for idx, full_path in input:
        path = Path(os.path.join(full_path, 'content.txt'))
        if path.exists():
            raw_data = path.read_text()
            raw_data = BeautifulSoup(raw_data, 'lxml').getText()
            words = re.sub(r'[^А-я0-9ёЁ ]', '', raw_data).split()
            words = [morph.parse(word)[0].normal_form for word in words if word not in stopwords.words('russian')]
            result = {}
            for i, word in enumerate(words):
                if word[0] == ch:
                    if word not in result:
                        result[word] = []
                    result[word].append(i)
            for key, val in result.items():
                if key not in dict_ch:
                    dict_ch[key] = []
                dict_ch[key].append((idx, val))

    with open(os.path.join('./index', ch), 'wb') as f:
        try:
            pickle.dump(dict_ch, f)
        except pickle.PicklingError as e:
            logging.error(str(e))


def clean_documents(document):
    for idx, fullpath in document:
        path = Path(os.path.join(fullpath, 'content.txt'))
        if path.exists():
            raw_data = path.read_text()
            raw_data = BeautifulSoup(raw_data, 'lxml').getText()
            words = re.sub(r'[^А-яёЁ ]', '', raw_data)
            with open(os.path.join(fullpath, 'clean_content.txt'), 'r') as f:
                f.write(words)



if __name__ == '__main__':
    html = """
    
<!DOCTYPE html>
<html>
<head>
<link rel="canonical" href="/diseases/cutis/karbunkul" />
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=10.0, user-scalable=yes" />
<title>Карбункул: симптомы, лечение - Диагноз.ру</title>
<meta name="keywords" content="Карбункул, лечение карбункула, карбункул фото">
<meta name="description" content="Симптомы карбункула с фото, методы его лечения, а также течение и прогноз при карбункуле.">
  <meta name='yandex-verification' content='49824300dcd5e505' />

<meta name="google-site-verification" content="hqtYdHBRCLFOg2tF_cDa9IzUfgj-eeRFhDR2YXA77d0" />
<meta name="google-site-verification" content="AlHjRu-6aheSjn3FJi2A9fcw7GUvuZNn-9MG0N0HPKc" />


<script async src="//pagead2.googlesyndication.com/pagead/js/adsbygoogle.js"></script>
<script>
  (adsbygoogle = window.adsbygoogle || []).push({
    google_ad_client: "ca-pub-6934910030428084",
    enable_page_level_ads: true
  });
</script>
 <style type="text/css">
@font-face{font-family:Roboto;font-style:normal;font-weight:400;src:local(Roboto), local(Roboto-Regular), url(http://fonts.gstatic.com/s/roboto/v15/bbysZXYlQ_uS6wRz---nZA.woff2) format(woff2), url(http://fonts.gstatic.com/s/roboto/v15/neaHiVpEUkuPmSawsrNWSg.woff) format(woff);}
</style>
<LINK rel="shortcut icon" href="/my/templates/2014/favicon.ico" type="image/x-icon">
<link rel=stylesheet href="/my/templates/2014/2014_teal_header_r300.css">
</head>
<body>
<div class="wrapper">
<header>
<div class="topwrap">
<div class="logo">
<a href="/" onFocus="this.blur()">
<img src="/my/templates/2014/Diagnos1sm.png" width="210" border=0>
</a>
</div>
<nav>
<div class="userbox">
<a href="/login/?upaction=login&refto=/diseases/cutis/karbunkul">Войти</a>
| <a href="/login/?upaction=register&refto=/diseases/cutis/karbunkul">Регистрация</a>
</div>
<ul class="menu0">
<li class="lmenu "><a href="/ddss/" class="yes ">
Диагностика</a>
<li class="lmenu y"><a href="/diseases" class="yes y">
Болезни</a>
<li class="lmenu "><a href="/symptoms" class="yes ">
Симптомы</a>
<li class="lmenu "><a href="/consult" class="yes ">
Вопрос доктору</a>
<li class="lmenu "><a href="/procedures" class="yes ">
Процедуры и анализы</a>
<li class="lmenu "><a href="/diet" class="yes ">
Диеты</a>
</ul>
</nav>
</div>
<div class="roomy">
<div class="topbanner">
</div>	<div class="crumbs">
<a href="/diseases">Болезни</a> &raquo; <a href="/diseases/cutis">Кожа, волосы, ногти</a> &raquo; </div>
<noindex><div class="cbannerr">
</div></noindex>
</div>
</header>
<!-- errmsg-->
<div class="adminka"><span class="admin_inpage">
<script language="javascript">
document_path="/diseases/cutis/karbunkul";
</script>
</span></div>
<div class="roomy" itemscope itemtype="http://schema.org/MedicalCondition">
<h1 itemprop="name">Карбункул</h1>
<article><div id="aktar_content">
<p itemprop=description >Карбункул — острое гнойное воспаление нескольких расположенных рядом сальных желёз и волосяных фолликулов, распространяющееся на окружающую кожу и подкожную клетчатку. Возбудителем карбункула является <a href="/diseases/infec/stafilokokk-gold">золотистый стафилококк</a>, часто в ассоциации с другими бактериями (протей, кишечная палочка).</p>
<p itemprop=description> Излюбленное его расположение - это задняя поверхность шеи, межлопаточная область, поясница, ягодицы, реже — конечности.</p>
<h2>Симптомы карбункула</h2>
<p itemprop=SignOrSymptom >Карбункул проявляется как небольшой воспалительный узелок с поверхностным гнойничком, который быстро увеличивается в размере. Часто в области поражения беспокоят резкие распирающие боли. Кожа в этой области напряжена, отёчна, с багровым оттенком.</p>
<p itemprop=SignOrSymptom >Через некоторое время кожа над карбункулом прорывается в нескольких местах, образуется несколько отверстий («сито»), из которых выделяется густой зеленовато-серый гной; в отверстиях видны омертвевшие ткани. Отдельные отверстия сливаются, образуя большой дефект в коже, через который вытекает много гноя и отторгаются мертвые ткани.</p>
<p itemprop=SignOrSymptom >Температура тела повышается до 40°С. Присутствует интоксикация (тошнота, рвота, потеря аппетита, сильная головная боль, <a href="/diseases/nerves/insomnia">бессонница</a>, изредка бред и бессознательное состояние).</p>
<p itemprop=naturalProgression >При больших размерах, а также при расположении карбункула на лице общие явления выражены особенно резко, но быстро идут на убыль, когда начинается выделение гноя и отторжение мёртвых тканей. После очищения рана заживает.</p>
<p><img alt="фото: карбункул" border="0" src="/my/img/a/carbuncle.jpg" title="внешний вид карбункула" /><br />
<em>Фото: внешний вид карбункула</em></p>
<h2>Лечение карбункула</h2>
<p>При лечении проводятся следующие мероприятия:</p>
<ul itemprop=possibleTreatment >
<li>Показан тщательный туалет кожи вокруг карбункула: 70% этиловый спирт + 2% салициловый спирт или 0,5—1% спиртовый раствор бриллиантового зелёного. В стадии формирования карбункул обрабатывают 5% спиртовым раствором йода.</li>
<li>Противомикробная терапия антибиотиками по показаниям. Применяются оксациллин, цефазолин в возрастных дозировках.</li>
<li>Физиолечение: УВЧ в количестве 10 процедур.</li>
<li>Хирургическое лечение: крестообразное рассечение и удаление мертвых тканей.</li>
</ul>
<p itemprop=possibleTreatment >При локализации карбункула выше угла рта и ниже угла глаза («злокачественный карбункул»), при наличии тяжёлых сопутствующих заболеваний (сахарный <a href="/diseases/endocrino/diabetes">диабет</a>, новообразования), выраженном синдроме интоксикации — госпитализация в палату интенсивной терапии.</p>
<p><a class="middlead2 tst17" name="middlead2"></a>
  
</p>
<p><b>Течение заболевания и прогноз</b></p>
<p itemprop=expectedPrognosis >При своевременном и правильном лечении прогноз благоприятный. У истощённых, ослабленных больных, страдающих тяжёлой формой сахарного диабета, а также при расположении карбункула на лице возможен летальный исход.</p>
</div></article>
</div>
  <script async src="//pagead2.googlesyndication.com/pagead/js/adsbygoogle.js"></script>
<!-- 2009-06-google-down -->
<ins class="adsbygoogle"
     style="display:inline-block;width:336px;height:280px"
     data-ad-client="ca-pub-6934910030428084"
     data-ad-slot="5095759681"></ins>
<script>
(adsbygoogle = window.adsbygoogle || <a class="" href="/login/w.htm?q=" class="wiki"></a>).push({});
</script>
 <div class="overfixbannerr">
<div class="fixbannerr">
  <script async src="//pagead2.googlesyndication.com/pagead/js/adsbygoogle.js"></script>
<!-- 300*600 -->
<ins class="adsbygoogle"
     style="display:inline-block;width:300px;height:600px"
     data-ad-client="ca-pub-6934910030428084"
     data-ad-slot="2588879575"></ins>
<script>
(adsbygoogle = window.adsbygoogle || []).push({});
</script>
 </div>
</div>
<div class="overnodiag">
<div id="nodiag">
<img src="/my/templates/img/close.gif" align=right onClick="$('#nodiag').hide('slow')">
<h3>Диагноз по симптомам</h3>
<p>
Узнайте ваши вероятные <b>болезни</b> и к какому <b>доктору</b> следует идти.
</p>
<p>
8-15 минут, есть бесплатный вариант.
</p>
<a href="/ddss/" class="diagenter">Пройти диагностику</a>
</div>
</div>
<script>
dshow = 0;
function nodiag(){
if( !dshow && typeof old_diag_needless == 'undefined'
&& document.location.href.indexOf('/ddss/') == -1 && document.location.pathname != "/"
&& document.cookie.indexOf("bolnoi") == -1
&& document.cookie.indexOf("aks") == -1
){
_ii = window.setTimeout(nd2,1500);
dshow = 1;
}
//	console.log("scroll");
}
function nd2(){
$("#nodiag").show("slow"); }
window.onscroll = nodiag;
</script>
</div>
<footer>
<div class="wrapper">
  (c) Diagnos.ru, 2002-2017. Все права защищены. Контакты: <script language=javascript>var dog="@";document.write ("<a href=mailto:ii.work"+dog+"mail.ru>ii.work"+dog+"mail.ru</a>")</script>. 

 <p>
<a href="https://plus.google.com/100116706016365646025" rel="publisher">Мы в Google+</a> &nbsp&nbsp&nbsp <a href='http://www.diagnos.ru/about'>О сайте</a>
  &nbsp; 
<a href="https://plus.google.com/100232203037485073718" rel="author"></a><br>


 <p>


<!--LiveInternet counter--><script language="JavaScript"><!--
document.write('<a href="http://www.liveinternet.ru/click" '+
'target=_blank><img src="http://counter.yadro.ru/hit?t26.13;r'+
escape(document.referrer)+((typeof(screen)=='undefined')?'':
';s'+screen.width+'*'+screen.height+'*'+(screen.colorDepth?
screen.colorDepth:screen.pixelDepth))+';u'+escape(document.URL)+
';'+Math.random()+
'" title="LiveInternet: показано число посетителей за сегодн\я" '+
'border=0 width=88 height=15></a>')//--></script><!--/LiveInternet -->

  <!-- Yandex.Metrika counter -->
<script type="text/javascript">
var yaParams = {/*Здесь параметры визита*/};
</script>

<div style="display:none;"><script type="text/javascript">
(function(w, c) {
    (w[c] = w[c] || []).push(function() {
        try {
            w.yaCounter11832181 = new Ya.Metrika({id:11832181, enableAll: true, trackHash:true, webvisor:true,params:window.yaParams||{ }});
        }
        catch(e) { }
    });
})(window, "yandex_metrika_callbacks");
</script></div>
<script src="//mc.yandex.ru/metrika/watch.js" type="text/javascript" defer="defer"></script>
<noscript><div><img src="//mc.yandex.ru/watch/11832181" style="position:absolute; left:-9999px;" alt="" /></div></noscript>
<!-- /Yandex.Metrika counter -->


  

<script>
  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');

  ga('create', 'UA-10732928-1', 'diagnos.ru');
  ga('require', 'displayfeatures');
  ga('send', 'pageview');

</script>

 </div>
</footer>
  <script async type="text/javascript" src="//sjsmartcontent.org/static/plugin-site/js/sjplugin.js" site="2lovnzx9zqu22tfh4gk">
</script>

<div>
<script>
  (function() {
    var cx = '011538744363723268679:leuizpkdv0a';
    var gcse = document.createElement('script');
    gcse.type = 'text/javascript';
    gcse.async = true;
    gcse.src = 'https://cse.google.com/cse.js?cx=' + cx;
    var s = document.getElementsByTagName('script')[0];
    s.parentNode.insertBefore(gcse, s);
  })();
</script>
<gcse:search></gcse:search>
</div>
 </body>
<script>
copyright_notice = "\n Подробнее на http://diagnos.ru/diseases/cutis/karbunkul";
</script>
<!-- -101500 -->	<script language="javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js"></script>
<!-- -990 -->	<link rel="stylesheet" href="/login/styles/ajax.css" type="text/css" />
<!-- -850 -->	<script language="javascript" src="/login/scripts/user.js"></script>
<!-- 10 -->	<script language="javascript" src="/my/templates/2014/2014_screen.js"></script>
<!-- 30 -->	<script language="javascript" src="/my/templates/2014/jquery.lightbox-0.5.js"></script>
<!-- 30 -->	<script language="javascript" src="/login/scripts/admin.js"></script>
<!-- 30 -->	<script>
	automenu();
</script>
<!-- 10000 -->	</html>

    """
    soup = BeautifulSoup(html, "lxml")
    title = soup.find_all('h1', {'itemprop': 'name'})
    result = ''
    for h2 in soup.find_all('h2'):
        if 'лечение' in h2.next.lower():
            for tag in h2.next_siblings:
                if tag.name == 'h2':
                    break
                elif tag == '\n':
                    result += tag
                else:
                    result += tag.text

    print(result)