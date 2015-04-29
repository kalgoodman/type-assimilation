package com.typeassimilation.scala

import org.eclipse.gef4.zest.fx.ui.parts.ZestFxUiView
import com.google.inject.Guice
import com.google.inject.util.Modules
import com.typeassimilation.example.ZestGraphExampleModule
import com.typeassimilation.example.ZestGraphExampleUiModule

class ScalalalaActualView extends ZestFxUiView(Guice
  .createInjector(Modules.`override`(new ZestGraphExampleModule()).`with`(new ZestGraphExampleUiModule()))) {
  setGraph(new ScalalalaView().createDefaultGraph())
}