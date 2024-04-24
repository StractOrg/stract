#!.venv/bin/python3
import requests
import pandas as pd
from tqdm import tqdm
import time

stopwords = [
    "the",
    "i",
    "we",
    "our",
    "he",
    "she",
    "they",
    "it",
    "a",
    "is",
]

queries = set(
    stopwords
    + [
        "season",
        "call",
        "aol mail login",
        "ao3",
        "france",
        "vanguard login",
        "kanye",
        "recipe",
        "zion",
        "garfield",
        "amazon customer service",
        "height",
        "apple stock",
        "qr code generator",
        "ups tracking",
        "uk",
        "lakers",
        "aioli",
        "food",
        "credit karma",
        "american express",
        "airline tickets",
        "adidas kanye",
        "kanye",
        "airbnb",
        "amazon",
        "python",
        "ant man",
        "aol mail",
        "apple stock",
        "apple",
        "ariana grande",
        "papa",
    ]
)


## exported from https://stract.com/explore
nsfw_optic = """DiscardNonMatching;
Rule {
	Matches {
		Site("|youporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|redtube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornhub.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|youjizz.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornone.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|eporner.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|4tube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|spankbang.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tube8.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xhamster.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xnxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porntrex.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xvideos.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|beeg.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porntube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|alohatube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porndig.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cliphunter.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gotporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hqporner.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pichunter.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gelbooru.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|luscious.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|thumbzilla.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porndoe.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|literotica.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornhd.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ixxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|imagefap.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|watchmygf.me|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tnaflix.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|simply-hentai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lobstertube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubegalore.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornpics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|drtuber.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornmd.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pictoa.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sxyprn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cartoonpornvideos.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nudevista.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ok.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|rule34.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentai2read.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|maturetube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porn300.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|3movs.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nhentai.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|findtubes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dinotube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaifox.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yuvutu.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|myhentaicomics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornsos.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fuskator.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ro89.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|camwhores.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mylust.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sex.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|melonstube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hanime.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zzcartoon.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hclips.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|thisvid.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|voyeurhit.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porndex.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|smutty.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hoodamateurs.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vipergirls.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porndish.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xmoviesforyou.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bellesa.co|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|shesfreaky.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|trannytube.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|multporn.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|8muses.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hotscope.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornbb.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|erosberry.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|txxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|analdin.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|megatube.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xanimeporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nude-gals.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vjav.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ashemaletube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tiava.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fuqer.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lushstories.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nifty.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|asmhentai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fuq.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubesafari.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|anysex.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fux.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|plusone8.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaigasm.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|freeadultcomix.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|animeidhentai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornstarbyface.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|miohentai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|allporncomic.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|muchohentai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dirtyship.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|naughtymachinima.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|camwhoresbay.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|shemalez.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sextvx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|boundhub.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|assoass.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|celebjihad.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|planetsuzy.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hqbabes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lesbianpornvideos.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xfantazy.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornhat.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cumlouder.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubepornstars.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|perfectgirls.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zoig.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porzo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|shooshtime.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|e-hentai.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fullporner.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kindgirls.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|recurbate.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hdzog.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pmatehunter.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|forhertube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|rexxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentai-moon.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaipros.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|3xplanet.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaidude.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vintage-erotica-forum.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexcelebrity.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexlikereal.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|flirt4free.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|trendyporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zbporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|24porn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fapmeifyoucan.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornky.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xpee.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|colegialasdeverdad.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornpaw.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|supjav.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|91porn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|reallifecam.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xmegadrive.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ohentai.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|avgle.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|videobox.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexvid.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|underhentai.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hitomi.la|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|celebsroulette.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jav.guru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|doujins.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|erome.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mypornstarbook.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|aznude.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porndune.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|shameless.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yespornplease.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|svscomics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|punishbang.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zzztube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|namethatporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|storiesonline.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornhits.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xxxtik.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|erofus.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornobae.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|deepfakeporn.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|feet9.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ebonypulse.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cambro.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jjgirls.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|thehentaiworld.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|crazyshit.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|duckgay.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|newestxxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|watchjavonline.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xyzcomics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|javbangers.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|arabysexy.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|4porn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|badjojo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|camshowdownload.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaifromhell.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|shegotass.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexalarab.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bigfuck.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornburst.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porn00.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornerbros.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|anyporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ghettotube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|adultdvdtalk.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|noodlemagazine.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cam4.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xtube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.holed.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tabootube.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tastyblacks.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lolhentai.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|faphouse.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|voyeurweb.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xtapes.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porcore.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|adultsearch.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|naughtyblog.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaicloud.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|daftsex.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|milffox.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|homemoviestube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|youramateurporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexstories.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|21sextury.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|keezmovies.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|youngpornvideos.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|blacked.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bigporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|heavy-r.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexygirlspics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fyptt.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mrdeepfakes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|perfectgirls.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|naoconto.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|myhentaigallery.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vrsmash.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mobifcuk.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|seaporn.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|inporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubepornclassic.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|landing.seancodynetwork.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kitty-kats.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tik.porn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornorips.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornolab.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tsumino.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xozilla.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|h-flash.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|coedcherry.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|novinhasdozapzap.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|anon-v.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xcafe.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubxporn.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|definebabe.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fun.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zdic.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jerkdude.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|primecurves.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gamcore.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|wankzvr.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|boobpedia.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kink.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|indiansexstories2.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|prothots.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexjk.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yourfreeporn.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hornywhores.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|titshits.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornplaybb.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|submityourflicks.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornbox.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porntn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|adultbay.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xxxvideos247.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nsfw247.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|myporn.club|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|goodporn.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|watch-my-gf.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaiheroes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cosplayporntube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vrlatina.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|whoreshub.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|influencersgonewild.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|letsjerk.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaipulse.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|thotslife.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|flyflv.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porngo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tiktits.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|similar.porn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornxs.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hpjav.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hdporn92.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cremz.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|femefun.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|rule34video.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|scandalplanet.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hobby.porn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hdporncomics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xkeezmovies.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fsicomics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kamababa.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|flingster.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|www5.javmost.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaihere.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|voyeur-house.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sucksex.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bdsmlibrary.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|thothub.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|veporno.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaiporns.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kissjav.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|landing.realitydudesnetwork.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|smutr.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porncomixonline.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|metaporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porndroids.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.seemygf.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hornysimp.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lovehomeporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porn555.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porn4days.biz|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cfake.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|milfnut.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gayforit.eu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|alotporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|poopeegirls.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubebdsm.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mompornonly.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fapvidhd.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|spicybigtits.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|landing.bromonetwork.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|faapy.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|antarvasnaclips.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fap-nation.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|camvideos.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|oncam.me|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|internetchicks.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sex4arabxxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|damplips.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|carameltube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lewdzone.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|iyottube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|homegrownfreaks.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|register.loveamateur.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|trannyvideosxxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|javfinder.la|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ruleporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|freesexyindians.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornjam.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ebonygalore.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xxxfree.watch|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pururin.io|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexsaoy.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|landing.sweetheartvideo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|enter.avanal.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xanimu.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|brasiltudoliberado.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|netfapx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubepleasure.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fikfap.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|3arabporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nxt-comics.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornbraze.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cartoonporno.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|freeomovie.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|javtiful.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornbay.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|overwatchporn.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornhd3x.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hotmovs.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|empflix.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|whentai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.anal4k.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|babesnetwork.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|julesjordan.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vrhush.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|free-codecs.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|alt.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|iwank.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|alphaporno.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sunporno.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xlovecam.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cams.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vxxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|inhumanity.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|watchhentai.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornolandia.xxx|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaifreak.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|humoron.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|theclassicporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|curvyerotic.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mybigtitsbabes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xfree.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexu.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porntop.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|siterips.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|babesource.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|freehdinterracialporn.in|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|javgg.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mult34.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|meetinchat.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tube.hentaistream.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|secure.anal-angels.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|eroticscribes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.baberotica.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lifeselector.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ebony8.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|upornia.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hdhole.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|assparade.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porncomix.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|access.trueanal.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bang.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|naughtyamericavr.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xnxxarab.cc|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hustler.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|virtualporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nakedpornpics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|t.aagm.link|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|myhentai.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|avn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornkai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pinkdino.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|expatistan.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dropmefiles.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|coolmath.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gomlab.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ezvid.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sweethome3d.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|brdteengal.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubewolf.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sislovesme.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|milfvr.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|huya.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|atspace.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|onlinefreecourse.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|privacypolicytemplate.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yespornpleasexxx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|slutroulette.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexycandidgirls.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|joylovedolls.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.girlcum.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.bbcpie.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vrporn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornoxo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|babepedia.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|analvids.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|digitalplayground.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|czechvr.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|asstr.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pussyspace.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|f95zone.to|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|0xxx.ws|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xxbrits.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|adultdeepfakes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nutaku.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|socialmediagirls.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pb-track.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|senzuri.tube|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ftvmilfs.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.amateursexteens.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|adultfilmdatabase.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|forum.adultdvdtalk.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hotgaylist.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|adultism.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|efukt.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|topescortbabes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mysexgames.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|porn-w.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jacquieetmicheltv.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentaiplay.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kemono.party|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cheggit.me|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gamesofdesire.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tgtube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|eroticmonkey.ch|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|forumophilia.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|playporngames.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xnalgas.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|coomeet.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|piratecams.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|skipthegames.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hentai.tv|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.myveryfirsttime.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tokyotosho.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|shemalestube.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|landing.mennetwork.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|boodigo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|thenipslip.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fappenist.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bigboobsalert.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lesbian8.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|extreme-board.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sexyandfunny.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.amateureuro.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|secure.anal-beauty.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|join.mamacitaz.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pics-x.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tubedupe.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fakku.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|javhd.today|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vipwank.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornedup.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pornmz.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tsescorts.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sankakucomplex.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|eccie.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xfollow.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|chyoa.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|justpicsplease.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|blazinglink.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|handjobhub.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|redditlist.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|trustedshopotc.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jiliblog.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dzenprinimatel.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraonlineslots.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mail-order-wives.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|shoppingcbd.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yuku.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gome.com.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|magix.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|anglican.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|art-psd.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofradeluxeslot.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tlumiki.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|schreibburo.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ferragamo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|infusionsoft.app|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|eacdn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|over40datingsites.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|eastafricangasoil.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|goodlayers.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|phpcms.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gotomoreinfo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|brinkster.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|websitetestlink.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|metu.edu.tr|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tempsite.ws|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|onlinemillionairedatingsites.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ozessay.com.au|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|aussie-pokies.club|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|y0.pl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|redwap.me|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|goserver.host|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofradeluxe2.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|makepolo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|virginmoneygiving.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ticksy.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gzgov.gov.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|spielen-bookofra.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|my3w.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cbdoilglobal.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|verycd.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|uuu9.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|topasianbrides.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|japanesemailorderbride.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|assignmenthelponline.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|unja.ac.id|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cscse.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|embedly.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|stratteramed.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|trusterworkonline.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xinhua.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|onesmablog.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|valtrexx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jussieu.fr|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|uny.ac.id|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraonlinespiele.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hotrussiangirls.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|c0.pl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-casino.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hookupguru.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|epower.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|home.ne.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|chello.nl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|moy.su|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofradownload.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|interracialdatingsitesreview.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|snnu.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tadalafil247.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cite4me.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tynt.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|blogbus.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|atorvastatinlipitor.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|as.me|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-tricks.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraspill.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|host.sk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|asianwifes.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|best-russian-women.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|clck.yandex.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|track.adsformarket.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|webnode.fr|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|synthroid20.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|advair1.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraonlinegratis.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|admission-essays.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|datarooms.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tinyblogging.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|officelive.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|papascoffee.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tomoreinformation.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|js.digestcolect.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|essaywriter.ca|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bestpornfinder.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gelocal.it|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|virtualave.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|infodeposit.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|iyiou.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jotform.me|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-onlinespielen.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|albuterolsale.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tsf-ftp.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|alicemchard.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|compaq.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tradetracker.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|webry.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|asianbrides.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|x10host.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|crsky.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|anonymouse.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|onlinespielebookofra.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ueuo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pandora.be|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cool.ne.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|szfw.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cyclopsinfosys.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|russianbrides.us|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|qhub.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dwcdn.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|voila.fr|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|clomidpill.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-online-play.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zhulong.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ucl.ac.be|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|datinglodge.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|urlperu.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra777.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pons.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|webportal.top|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraplayonline.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|neu.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|news12.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zhcw.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ventolinalb.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|orgfree.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|blackentertainments.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|antagroup.mn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ccm.gov.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|osdn.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cecdc.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bestrealdatingsites.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yandex.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fivehealthtips.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nlc.gov.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mee.nu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|wikia.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|garv.in|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|besthookupsites.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nodak.edu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gkstk.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|typecho.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|my-free.website|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofracards.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|eu5.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yjtag.yahoo.co.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|websitehome.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|salsalabs.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mybeautybrides.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zcool.com.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|asiandatingreviews.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|camgirls1.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zodiyak.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|miaopai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vxinyou.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pointblog.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tillerrakes.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|asianbrides.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|essaywriterforyou.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-paradise.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lz13.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|suhagra2020.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofrakostenlosspiele.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ultius.ws|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zoloftgen.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|blastingnews.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cbdoiladvice.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|asian-singles.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lisinopriltab.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nn.pe|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|casinopokies777.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|flywheelsites.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|efu.com.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|poco.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|essaypro.ws|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vox-cdn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|playbookofra.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hotcamgirls1.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bendibao.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraslot.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|chinagwy.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ataraxgen.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|food.blog|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bieberclub.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cas.cz|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraonlinespiele.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|prz.edu.pl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hotbride.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|odn.ne.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|serving-sys.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|trusterworkshop.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|scene7.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gitbooks.io|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pe.hu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xuite.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|els-cdn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bestasianbrides.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraspielenonline.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|csu.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zhiye.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|plaquenilhydroxychloroquine.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|csair.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|adsformarket.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|toponlinedatingservices.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|chsi.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dontstopthismusics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|evolutionwriters.biz|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofrasecret.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cheapestpricesale.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|plantronics.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|paipai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|celebrexcap.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|swipnet.se|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-gratis.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ui.ac.id|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|writtingessays.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|wifeo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|by.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|b.yjtag.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tw1.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|retinaotc.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|studa.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|playpokiesfree.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hausarbeit-ghostwriter.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yhd.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hbtv.com.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|privatewriting.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|geovisit.ge|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|btinternet.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ccam.org.ar|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vmall.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cbdoilrank.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|blog2learn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cngold.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mail-order-wife.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|chemnet.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraspiele.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofradeluxeslot.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hfut.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vietvoters.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|legitmailorderbride.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sitey.me|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|memberclicks.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|planet.nl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|activehosted.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraonlinespielen.online|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zjut.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|umm.ac.id|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|umk.pl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|slidesharecdn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|onlinebookofraspielen.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dreamessaywriter.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|beep.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|radiovaticana.va|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-online-tricks.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cbdoilworld.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|public.lu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|customessays.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|vhostgo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|datingstudio.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cipro360.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|diowebhost.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraonlinegame.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yjtag.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|multiscreensite.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraspelen.nl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-online-game.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|forumcrea.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kamagra.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|clara.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|russian-women-dating.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|russianbridesfinder.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|playdadnme.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ukrainakomi.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bizland.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|adult-friend-finder.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bbci.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ukraine-women.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|casino-bonus-free-money.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|undip.ac.id|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|acyclovirzov.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gamersky.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|galegroup.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cofc.edu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|diflucanmed.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ptcgeneration.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yourrussianbride.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tokyo.lg.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cbdoildiscount.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ks.gov|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pandora.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ebc.com.br|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|v.calameo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookoframobile.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|oh100.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dlut.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tretinoinsale.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|meitu.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraspel.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|djpodgy.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|err.ee|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gameforge.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|transip.eu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mail-order-brides-sites.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|on.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sm.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pfu.edu.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|domainprofi.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|to8to.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|teacup.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ccnu.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|blogolize.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bizrate.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lazaworx.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|digod.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nenu.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|scut.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|domyhomeworkfor.me|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|scriptalicious.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|healthfully.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|dating-ukrainian-brides.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|stagram.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kennesaw.edu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|buyabrideonline.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|valorus-advertising.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|wwitv.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|maldimix.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|tv-tv-lv.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-online-spielen.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|blogcn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|amoxicillinbio.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bestlatinabrides.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|wedoyouressays.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|moscow-brides.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fontsly.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-player.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|rankmywriter.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|doxycycline360.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|embassy.gov.au|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hostingwijzer.nl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-topliste.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|wd.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jnu.edu.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|albendazoleotc.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|besthookupssites.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|propecialab.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|serving.com.ec|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hospedagemdesites.ws|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ocnk.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|whataboutloans.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|synonym.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jewishdatingsites.biz|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra88.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|antabusedsuf.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|msgfocus.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hookupwebsites.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cheapestnetshop.info|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|npage.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|russianbrideswomen.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|baofeng.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|monequateur.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bestlatinwomen.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|aptoide.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kym-cdn.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ym.edu.tw|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|web-pods.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jstv.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-novoline.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cbdoilmarkets.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hunantv.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|qihoo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|es.tl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|haicuneo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|atwebpages.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hs-sites.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|brightbrides.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|isrefer.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mcu.edu.tw|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|suzhou.gov.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|karelia.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|logi.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|rankingsandreviews.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|writing-online.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lockware.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|agri.gov.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|centerblog.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|puzl.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|charmingbrides.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraspielenonline.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|foreign-brides.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-gratis.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ogtk.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|casino-online-australia.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bitcoin.it|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|metformingluc.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|prohosting.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofragratuit.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|freeslotsnodownload-ca.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofrafree.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|wps.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|douyu.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|phorum.pl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|netlog.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|indocinmed.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|googlesource.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fd556.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gcs-web.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cbdoilrank.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|myway.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|naturalwellnesscbdoil.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|jouwweb.nl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|besthookup.reviews|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pcgames.com.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ok365.com.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraohnelimits.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|diegoassandri.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|showartcenter.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|funpic.de|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mtvnservices.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-spiel.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gloriousbride.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bestrussianbrides.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|essaywriter24.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kaywa.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|nthu.edu.tw|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|custom-writing.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bestforeignbride.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofradeluxekostenlosspielen.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|freehookup.reviews|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ne.gov|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|gridhosted.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ugu.pl|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|fsnet.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|myfreepokies.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mumayi.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|userapi.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|payforpapers.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|loverusbrides.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|commnet.edu|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mensfitness.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yinyuetai.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|pchouse.com.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|badcreditloanapproving.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|sina.com.tw|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|paytowritemyessay.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hot-russian-women.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|maps.ie|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|webnode.es|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kamagraxr.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|akademitelkom.ac.id|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|zjaic.gov.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|yar.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|php-editors.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|ceair.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|wellbutrinlab.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|findmailorderbride.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|real-money-casino.club|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|lexapro10.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|timepad.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|xnxxxv.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|hospitalathome.it|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|extreme-dm.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|destinyfernandi.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|competitor.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|o2.co.uk|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bofilm.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|qpic.cn|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|livefilestore.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cbdoildelivery.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofra-online.cc|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bridepartner.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|php-myadmin.ru|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|theplatform.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|kir.jp|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofraspiele.net|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|u-strasbg.fr|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|f2s.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|cudasvc.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|mail-order-bride.org|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|bookofrainfo.com|"),
	}	Action(Boost(0))
};
Rule {
	Matches {
		Site("|scnu.edu.cn|"),
	}	Action(Boost(0))
};
Like(Site("pornhub.com"));
Like(Site("youporn.com"));
Like(Site("redtube.com"));
"""


def search(json):
    json["numResults"] = 50
    r = requests.post(
        "https://stract.com/beta/api/search",
        json=json,
    ).json()

    time.sleep(5)

    return r


def search_nsfw(q):
    return search({"query": q, "optic": nsfw_optic})


def search_sfw(q):
    return search({"query": q})

def snippet_text(snip):
    return ''.join([frag['text'] for frag in snip['text']['fragments']])


def content(search_results):
    return [
        {"url": result["url"], "text": result["title"] + " " + snippet_text(result["snippet"])}
        for result in search_results["webpages"]
    ]


data = {"query": [], "url": [], "text": [], "nsfw": []}

for query in tqdm(queries):
    results = content(search_nsfw(query))
    for result in results:
        data["query"].append(query)
        data["url"].append(result["url"])
        data["text"].append(result["text"])
        data["nsfw"].append(True)

    results = content(search_sfw(query))
    for result in results:
        data["query"].append(query)
        data["url"].append(result["url"])
        data["text"].append(result["text"])
        data["nsfw"].append(False)


df = pd.DataFrame(data)

df = df.drop_duplicates(subset=["url"])

df = df.rename(columns={"text": "text", "nsfw": "label"})
df["label"] = df["label"].map({True: "NSFW", False: "SFW"})
df = df[["text", "label"]]

df.to_csv("data/nsfw.csv", index=False)
